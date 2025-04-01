use std::{collections::{HashMap, HashSet}, io::Write};

use clap::Parser;
use deunicode::deunicode;
use futures::StreamExt;

const PAGINEGIALLE_URL: &'static str = "https://www.paginegialle.it";
const PAGINEGIALLE_CATEGORIE_URL: &'static str = "https://www.paginegialle.it/categorie.htm";
const COMUNI_API_URL: &'static str = "https://axqvoqvbfjpaamphztgd.functions.supabase.co/comuni/provincia";
const DEFAULT_PAGE_LIMIT: usize = 10;
const DEFAULT_REQUESTS_BATCH: usize = 50;

#[derive(Debug, PartialEq, Eq, Hash, serde::Serialize)]
struct BusinessEntry {
    name: String,
    address: String,
    phones: String,
}

#[derive(clap::Parser)]
#[command(about = "Scrapes PagineGialle businesses data into a csv file. Puntuactions should be replaced with _")]
struct Cli {
    region: String,
    city: String,

    /// if left empty, will scrape for ALL PagineGialle categories (might be very slow)
    category: Option<String>,

    /// output filename (without the .csv extension)
    #[arg(default_value = "output")]
    output_file: String,

    /// maximum pages to be scraped for each city
    #[arg(default_value_t = DEFAULT_PAGE_LIMIT)]
    page_limit: usize,
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

async fn get_all_categories(client: &reqwest::Client) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let html = client.get(PAGINEGIALLE_CATEGORIE_URL)
        .send().await?.text().await?;
    let document = scraper::Html::parse_document(&html);

    let category_selector = scraper::Selector::parse(".categorie__item")?;
    
    let categories = document.select(&category_selector)
        .map(|e| e.text().collect::<String>().trim().to_string())
        .map(|s| s.to_lowercase().replace(|c: char| c.is_whitespace() || c.is_ascii_punctuation(), "_"))
        .collect();

    Ok(categories)
    
    // let category_page_selector = scraper::Selector::parse(".categorie__item--show a")?;
    // let subcategory_selector = scraper::Selector::parse(".categorie-macro__box-corr__itm a")?;
    // let categories = document.select(&category_page_selector)
    //     .map(|e| e.attr("href").unwrap_or_default())
    //     .filter(|e| !e.is_empty())
    //     .collect::<Vec<_>>();

    // let mut subcategories = Vec::new();

    // // TODO: this is not async
    // for subcategory_url in categories {
    //     let html = client.get(subcategory_url).send().await?.text().await?;
    //     let document = scraper::Html::parse_document(&html);

    //     document.select(&subcategory_selector)
    //     .map(|e| e.text().collect::<String>().trim().to_string())
    //     .map(|s| s.to_lowercase().replace(|c: char| c.is_whitespace() || c.is_ascii_punctuation(), "_"))
    //     .for_each(|c| subcategories.push(c));
    // }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // parse command line args
    let region = cli.region;
    let provincia = cli.city;
    let category = cli.category.unwrap_or_default();
    let page_limit = cli.page_limit;
    let output_filename = cli.output_file;
    let mut output_path = std::path::PathBuf::new();
    output_path.push(output_filename);
    output_path.set_extension("csv");

    // fetch comuni list from api
    let comuni_url = format!("{COMUNI_API_URL}/{provincia}?format=csv&onlyname=true");
    let comuni_csv = reqwest::get(comuni_url).await?.text().await?;
    
    let mut comuni: Vec<String> = csv::Reader::from_reader(comuni_csv.as_bytes())
    .into_deserialize()
    .map(|e| e.unwrap_or_default())
    .map(|s: String| s.trim_end_matches(|c: char| c.is_ascii_punctuation())
        .replace(|c: char| c.is_whitespace(), "_")
        .replace(|c: char| c.is_ascii_punctuation(), "_")
        .to_lowercase())
    .map(|s: String| deunicode(&s))
    .collect();

    if comuni.is_empty() {
        comuni.push(provincia);
    }

    println!("Comuni da ricercare:\n{comuni:?}\n\nLimite pagine: {page_limit}");

    // build http fetching tasks
    let client = reqwest::Client::new();
    
    let categories = if category.is_empty() {
        println!("Nessuna categoria specificata. Saranno ricercate ditte per TUTTE le categorie (potrebbe impiegare molto tempo).");
        get_all_categories(&client).await?
    } else { vec![category] };

    println!("{categories:?}");

    // build paginegialle urls to scrape
    let mut urls = Vec::new();
    for category in categories {
        for comune in &comuni {
            for i in 0..page_limit {
                let url = format!("{PAGINEGIALLE_URL}/{region}/{comune}/{category}/p-{i}.html");
                urls.push(url);
            }
        }
    }
    
    // println!("Url generati: {urls:?}");
    println!("Richieste da effettuare: {}\n", urls.len());

    // https://stackoverflow.com/questions/51044467/how-can-i-perform-parallel-asynchronous-http-get-requests-with-reqwest/51047786#51047786
    let htmls = futures::stream::iter(&urls).enumerate()
    .map(|(i, url)| {
        // if i % (urls.len() / 100) == 0 {
        //     let percentage = ((i as f32 / urls.len() as f32) * 100.0).round();
        //     print!("\r{percentage:02}% completato, {i} richieste effetuate");
        //     std::io::stdout().flush().unwrap();
        // }
        let percentage = ((i as f32 / urls.len() as f32) * 100.0).round();
        print!("\r{percentage:>2}% completato, {} richieste effetuate", i+1);
        std::io::stdout().flush().unwrap();

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
                
                let mut elements = document
                    .select(&entries_selector)
                    .peekable();

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

    let mut entries = HashSet::new();
    let mut errors = HashMap::new();

    // receive data from tasks
    while let Ok(res) = receiver.recv() {
        match res {
            Ok(entry) => {
                if !entry.name.is_empty() && !entry.phones.is_empty() {
                    entries.insert(entry);
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

    let mut entries = entries.into_iter().collect::<Vec<_>>();
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
