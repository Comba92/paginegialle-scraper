use std::{collections::HashMap, io::Write};

use deunicode::deunicode;
use futures::StreamExt;

const PAGINEGIALLE_URL: &'static str = "https://www.paginegialle.it";
const COMUNI_API_URL: &'static str = "https://axqvoqvbfjpaamphztgd.functions.supabase.co/comuni/provincia";
const DEFAULT_PAGE_LIMIT: usize = 20;
const DEFAULT_REQUESTS_BATCH: usize = 50;

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
struct BusinessEntry {
    name: String,
    address: String,
    phones: String,
}

fn extract_data_from_html(element: &scraper::ElementRef, selector: &scraper::Selector) -> String {
    let html = element.select(selector)
        .next()
        .map(|e| e.inner_html().trim().to_string())
        .unwrap_or_default();

    let frag = scraper::Html::parse_fragment(&html);
    let mut tokens = Vec::new();

    // https://users.rust-lang.org/t/removing-html-tags-from-a-string-obtained-from-select-crate/45000
    for node in frag.tree {
        if let scraper::node::Node::Text(text) = node {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                tokens.push(trimmed.to_string()); 
            }
        }
    }

    tokens.join(" ")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args();

    if args.len() < 4 {
        print!("Usage: region city category [page-limit] [output-filename]");
        std::process::exit(0);
    }

    // parse command line args
    let region = args.nth(1).unwrap();
    let provincia = args.next().unwrap();
    let category = args.next().unwrap_or_default();
    let page_limit = args.next().unwrap_or_default().parse().unwrap_or(DEFAULT_PAGE_LIMIT);
    let output_filename = args.next().unwrap_or(String::from("output"));
    let mut output_path = std::path::PathBuf::new();
    output_path.push(output_filename);
    output_path.set_extension("csv");


    // fetch comuni list from api
    let comuni_url = format!("{COMUNI_API_URL}/{provincia}?format=csv&onlyname=true");
    let comuni_csv = reqwest::get(comuni_url).await?.text().await?;
    
    let comuni: Vec<String> = csv::Reader::from_reader(comuni_csv.as_bytes())
    .into_deserialize()
    .map(|e| e.unwrap_or_default())
    .map(|s: String| s.trim_end_matches(|c: char| c.is_ascii_punctuation())
        .replace(|c: char| c.is_whitespace(), "_")
        .replace(|c: char| c.is_ascii_punctuation(), "_")
        .to_lowercase())
    .map(|s: String| deunicode(&s))
    .collect();

    println!("Comuni da ricercare:\n{comuni:?}\n\nLimite pagine: {page_limit}");
    

    // build paginegialle urls to scrape
    let mut urls = Vec::new();
    for comune in &comuni {
        for i in 0..page_limit {
            let url = format!("{PAGINEGIALLE_URL}/{region}/{comune}/{category}/p-{i}.html");
            urls.push(url);
        }
    }
    
    println!("Richieste da effettuare: {}\n", urls.len());
    

    // build http fetching tasks
    let client = reqwest::Client::new();

    // https://stackoverflow.com/questions/51044467/how-can-i-perform-parallel-asynchronous-http-get-requests-with-reqwest/51047786#51047786
    let htmls = futures::stream::iter(&urls).enumerate()
    .map(|(i, url)| {
        if i % (urls.len() / 10) == 0 {
            print!("\r{}% completato", ((i as f32 / urls.len() as f32) * 100.0).round());
            std::io::stdout().flush().unwrap();
        }

        let client = client.clone();
        async move {
            let res = client.get(url).send().await?;
            let url = res.url().to_string();
            Ok((res.text().await?, url))
        }
    })
    .buffer_unordered(DEFAULT_REQUESTS_BATCH);

    let entries_selector = scraper::Selector::parse(".search-itm")?;
    let business_name_selector = scraper::Selector::parse(".search-itm__rag")?;
    let address_selector = scraper::Selector::parse(".search-itm__adr")?;
    let phone_selector = scraper::Selector::parse(".search-itm__phone")?;
    
    let (sender, receiver)  = std::sync::mpsc::channel();


    // scrape data from html text
    htmls.for_each(|response: Result<_, reqwest::Error>| async {
        match response {
            Ok((html, url)) => {
                let document = scraper::Html::parse_document(&html);
                
                let mut elements = document.select(&entries_selector).peekable();
                if elements.peek().is_none() {
                    let comune = url.split('/').rev().nth(2).unwrap_or_default();
                    sender.clone().send(Err(comune.to_string())).unwrap();
                    return;
                }

                for element in elements {
                    let name = extract_data_from_html(&element, &business_name_selector);
                    // https://stackoverflow.com/questions/71864137/whats-the-ideal-way-to-trim-extra-spaces-from-a-string
                    let address = extract_data_from_html(&element, &address_selector).split_whitespace().collect::<Vec<_>>().join(" ");
                    let phones = extract_data_from_html(&element, &phone_selector);

                    let phones = phones.split_whitespace()
                        .collect::<Vec<_>>()
                        .windows(2)
                        .map(|w| format!("{}-{}", w[0], w[1]))
                        .collect::<Vec<_>>()
                        .join(" | ");

                    let entry = BusinessEntry { name, phones, address };
                    sender.clone().send(Ok(entry)).unwrap();
                }
            }
            Err(e) => eprintln!("Errore non gestito per: {e}"),
        }
    }).await;

    // the upper level sender is not used, it should be dropped so that the receiver knows when there are no more senders
    drop(sender);

    let mut entries = Vec::new();
    let mut errors = HashMap::new();

    // receive data from tasks
    while let Ok(res) = receiver.recv() {
        match res {
            Ok(entry) => {
                if !entry.name.is_empty() && !entry.phones.is_empty() {
                    entries.push(entry);
                }
            }
            Err(e) => {
                let count = errors.get(&e).map_or(1, |n| n+1);
                errors.insert(e, count);
            }
        }
    }

    let not_found = errors.iter().filter(|(_, &val)| val == page_limit).map(|(key, _)| key).collect::<Vec<_>>();
    if !not_found.is_empty() {
        eprint!("\r");

        if not_found.len() == comuni.len() {
            eprint!("Nessuna provincia ha ottenuto alcun risultato. Hai scelto una categoria di attivita' valida?");
            return Ok(());
        } else {
            eprintln!("Nessun risultato per le seguenti provincie: {not_found:?}");
        }
    }

    println!("\rScraping finito, salvataggio su file CSV...");

    entries.sort_by_key(|e| (e.name.to_lowercase(), e.address.to_lowercase()));
    entries.dedup_by(|a, b| a == b);

    let mut csv_writer = csv::WriterBuilder::new().flexible(false)
        .from_path(output_path)?;
    for entry in entries {
        csv_writer.serialize(entry)?;
    }
    csv_writer.flush()?;
    
    Ok(())
}
