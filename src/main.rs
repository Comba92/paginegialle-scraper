use std::{collections::{HashMap, HashSet}, io::Write};

use clap::Parser;
use deunicode::deunicode;
use futures::StreamExt;

const PAGINEGIALLE_URL: &'static str = "https://www.paginegialle.it";
const PAGINEGIALLE_CATEGORIE_URL: &'static str = "https://www.paginegialle.it/categorie.htm";
const COMUNI_API_URL: &'static str = "https://axqvoqvbfjpaamphztgd.functions.supabase.co/comuni/";
const DEFAULT_PAGE_LIMIT: usize = 5;
const DEFAULT_REQUESTS_BATCH: usize = 50;

#[derive(Debug, PartialEq, Eq, Hash, serde::Serialize)]
struct BusinessEntry {
    name: String,
    address: String,
    phones: String,
    whatsapp: Option<String>,
    contact_url: Option<String>,
}

#[derive(clap::Parser)]
#[command(version, about = "Scrapes PagineGialle businesses data into a csv file. Puntuactions should be replaced with _")]
struct Cli {
    /// which kind of search to request
    #[command(subcommand)]
    mode: CliMode,

    /// output filename (without the .csv extension)
    #[arg(short, long = "output", default_value = "output")]
    output_file: String,

    /// maximum pages to be scraped for each query
    #[arg(short = 'l', long = "limit", default_value_t = DEFAULT_PAGE_LIMIT)]
    page_limit: usize,

    /// show debugging info
    #[arg(short, long)]
    debug: bool,
}

/*
    TWO KINDS OF URLS:
    kind one:
        https://www.paginegialle.it/<regione>/<citta>/<categoria>.html

        requires: regione, citta, categoria
        support for whole region

    kind two: 
        https://www.paginegialle.it/ricerca/<search>/[<citta>]
        (no .hmtl at end!)

        requires: search, citta 
*/
#[derive(clap::Subcommand)]
enum CliMode {
    /// Needs a search query, and optionally a city.
    /// (urls of kind `https://www.paginegialle.it/<regione>/<citta>/<categoria>.html`)
    Search(SearchMode),
    /// Needs a region name, and optionally a city and a category.
    /// (urls of kind `https://www.paginegialle.it/ricerca/<search>/[<citta>]`)
    Filter(FilterMode),
}

#[derive(clap::Args)]
struct SearchMode {
    /// search query, should be a business category or a business name
    query: String,
    /// location to search businesses in (might be city or region)
    location: Option<String>,
}

#[derive(clap::Args)]
struct FilterMode {
    /// region to search businesses in
    region: String,

    /// city to search businesses in
    /// if left empty, will scrape for ALL cities in the region
    city: Option<String>,

    #[arg(long)]
    /// if city provided is a province (example: Padova), setting this flag will scrape all cities in the province.
    /// If city is not a province, this flag does nothing
    all_cities: bool,

    #[arg(short, long)]
    /// business category to search for
    category: Option<String>,
}

// TODO: consider caching these (they are static data)
async fn get_all_categories() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // THIS ONLY GETS THE MOST POPULAR CATEGORIES
    let html = reqwest::get(PAGINEGIALLE_CATEGORIE_URL)
        .await?.text().await?;
    let document = scraper::Html::parse_document(&html);

    let category_selector = scraper::Selector::parse(".categorie__item")?;
    
    let categories = document.select(&category_selector)
        .map(|e| e.text().collect::<String>().trim().to_string())
        .map(|s| s.to_lowercase().replace(|c: char| c.is_whitespace() || c.is_ascii_punctuation(), "_"))
        .collect();

    Ok(categories)
    

    // THIS GETS ALL CATEGORIES
    // This collects too many pages.
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

fn sanitize_comune_str(comune: &str) -> String {
    let s = comune.trim_end_matches(|c: char| c.is_ascii_punctuation())
        .replace(|c: char| c.is_whitespace(), "_")
        .replace(|c: char| c.is_ascii_punctuation(), "_")
        .to_lowercase();

    deunicode(&s)
}

async fn generate_urls_with_filter_mode(params: &FilterMode, limit: usize, debug: bool) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
    /*
        Casi:
        1. Solo regione, cerca in tutte le provincie
        2. Regione e provincia, cerca solo nella citta di provincia o cerca in tutti i comuni
        3. Regione e citta, cerca solo nel comune
    */

    let comuni = match &params.city {
        Some(city) => {
            // cerca in una sola citta oppure controlla se e' provincia

            // fetch comuni list from api
            // let comuni_url = format!("{COMUNI_API_URL}/provincia/{city}?format=csv&onlyname=true");
            let comuni_url = format!("{COMUNI_API_URL}/provincia/{city}?format=csv");
            let comuni_csv = reqwest::get(comuni_url).await?.text().await?;


            // we try to filter out cities with fewer inhabitatns, to get fewer requests to make
            #[derive(serde::Deserialize, Debug, Default)]
            struct Comune {
                nome: String,
                popolazione: usize,
            }
            
            let mut comuni_de = csv::ReaderBuilder::new()
                .delimiter(b';')
                .from_reader(comuni_csv.as_bytes());

            let mut comuni = comuni_de.deserialize::<Comune>()
                .map(|e| e.unwrap_or_default())
                .collect::<Vec<_>>();

            comuni.sort_by_key(|c| std::cmp::Reverse(c.popolazione));
            comuni.drain(comuni.len()/2 + comuni.len()/3..);

            let comuni = comuni.into_iter()
                .map(|c| sanitize_comune_str(&c.nome))
                .collect::<Vec<_>>();

            if comuni.is_empty() || !params.all_cities {
                // ricerca per singola citta
                vec![city.clone()]
            } else {
                // ricerca per tutta provincia

                // aggiungo anche la provincia (dovrebbe gia esser)
                // comuni.push(city.to_string());
                comuni
            }
        }
        
        None => {
            // cerca in tutta la regione
            let comuni_url = format!("{COMUNI_API_URL}/regione/{region}?format=csv&onlyname=true", region = params.region);
            let comuni_csv = reqwest::get(comuni_url).await?.text().await?;
            let comuni = csv::Reader::from_reader(comuni_csv.as_bytes())
                .into_deserialize::<String>()
                .map(|e| e.unwrap_or_default())
                .map(|s| sanitize_comune_str(&s))
                .collect::<Vec<_>>();

            comuni
        }
    };
    
    println!("Comuni da ricercare:\n{comuni:?}\n");
    
    let categories = if let Some(category) = &params.category {
        vec![category.clone()]
    } else {
        println!("Nessuna categoria specificata. Saranno ricercate ditte per TUTTE le categorie seguenti (potrebbe impiegare molto tempo).");
        get_all_categories().await?
    };

    if debug {
        println!("Categorie da ricercare:\n{categories:?}\n");
    }

    let mut urls = Vec::new();
    for category in categories {
        for comune in &comuni {
            let base = format!("{PAGINEGIALLE_URL}/{region}/{comune}/{category}/", region = params.region);
            for i in 0..limit {
                let url = format!("{base}/p-{i}.html");
                urls.push(url);
            }
        }
    }

    Ok((urls, comuni))
}

fn generate_urls_with_search_mode(params: &SearchMode, limit: usize) -> Vec<String> {
    let mut base = format!("{PAGINEGIALLE_URL}/ricerca/{}", params.query);
    if let Some(city) = &params.location {
        base.push('/');
        base.push_str(&city);
    } else {
        println!("Nessuna citta' provveduta; la ricerca verra' eseguita in tutta Italia.")
    }
    
    let mut urls = Vec::new();
    for i in 0..limit {
        let url = format!("{base}/p-{i}");
        urls.push(url);
    }

    urls
}

fn extract_text_from_html(element: &scraper::ElementRef, selector: &scraper::Selector) -> String {
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
    let cli = Cli::parse();
    
    // TODO: write file in real time?
    let output_filename = cli.output_file;
    let mut output_path = std::path::PathBuf::new();
    output_path.push(output_filename);
    output_path.set_extension("csv");

    let (urls, comuni) = match cli.mode {
        CliMode::Search(ref params) => {
            (generate_urls_with_search_mode(params, cli.page_limit), vec![])
        }
        CliMode::Filter(ref params) => {
            generate_urls_with_filter_mode(params, cli.page_limit, cli.debug).await?
        }
    };
    
    if cli.debug {
        println!("Url generati: {urls:?}\n");
    }
    println!("Richieste da effettuare: {}", urls.len());

    let timer_start = std::time::Instant::now();

    // https://stackoverflow.com/questions/51044467/how-can-i-perform-parallel-asynchronous-http-get-requests-with-reqwest/51047786#51047786
    let client = reqwest::Client::new();

    // THIS JUST SENDS THE HTTP REQUESTS
    let htmls = futures::stream::iter(&urls)
    .enumerate()
    .map(|(i, url)| {
        // if i % (urls.len() / 100) == 0 {
        //     let percentage = ((i as f32 / urls.len() as f32) * 100.0).round();
        //     print!("\r{percentage:02}% completato, {i} richieste effetuate");
        //     std::io::stdout().flush().unwrap();
        // }

        // TODO: this just counts the requests sent, not the responses received...
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
    let whatsapp_selctor = scraper::Selector::parse("a[data-pag=\"whatsapp\"]")?;
    let contact_select = scraper::Selector::parse("#contattaci_btn")?;

    let (sender, receiver)  = std::sync::mpsc::channel();

    // scrape data from html text
    // THIS PARSES THE HTTP RESPONSES TEXT
    htmls.for_each(|response: Result<_, reqwest::Error>| async {
    match response {
        Ok((html, url)) => {
            // TODO: would be a great idea to factor out into a function
            let document = scraper::Html::parse_document(&html);
            
            let mut elements = document
                .select(&entries_selector)
                .peekable();

            if elements.peek().is_none() {
                match &cli.mode {
                    CliMode::Search(_) => {
                        // we don't care about errors here
                    }
                    CliMode::Filter(_) => {
                        let comune = url.split('/').rev().nth(2).unwrap_or_default();
                        sender.clone().send(Err(comune.to_string())).unwrap();
                    }
                }

                return;
            }

            for element in elements {
                let name = extract_text_from_html(&element, &business_name_selector);
                // https://stackoverflow.com/questions/71864137/whats-the-ideal-way-to-trim-extra-spaces-from-a-string
                let address = extract_text_from_html(&element, &address_selector).split_whitespace().collect::<Vec<_>>().join(" ");
                let phones = extract_text_from_html(&element, &phone_selector);

                let phones = phones.split_whitespace()
                    .collect::<Vec<_>>()
                    .windows(2)
                    .map(|w| format!("{}-{}", w[0], w[1]))
                    .collect::<Vec<_>>()
                    .join(" | ");

                let whatsapp = element.select(&whatsapp_selctor)
                    .next()
                    .map(|n| n.attr("href").map(|s| s.to_string()).unwrap_or_default())
                    .map(|s| s.chars()
                        .skip_while(|c| *c != '+')
                        .take_while(|c| c.is_numeric())
                        .collect()
                    );

                let contact_url = element.select(&contact_select)
                    .next()
                    .map(|n| n.attr("href").map(|s| s.to_string()).unwrap_or_default());

                let entry = BusinessEntry { name, phones, address, whatsapp, contact_url };
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
                // let count = errors.get(&e).map_or(1, |n| n+1);
                // errors.insert(e, count);

                errors.entry(e)
                    .and_modify(|n| { *n += 1; })
                    .or_insert(1);
            }
        }
    }

    // TODO: fix error reporting
    let not_found = errors.iter()
        .filter(|(_, &val)| val == cli.page_limit)
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    
    if !not_found.is_empty() {
        eprint!("\r");

        match cli.mode {
            CliMode::Search(_)  => {}
            CliMode::Filter(_) => {
                if not_found.len() == comuni.len() {
                    eprint!("Nessuna provincia ha ottenuto alcun risultato. Hai scelto una categoria di attivita' valida?");
                    return Ok(());
                } else {
                    eprintln!("Nessun risultato per le seguenti provincie: {not_found:?}");
                }
            }
        }
    }

    let time_took = std::time::Instant::now() - timer_start;
    println!("Time took: {time_took:?}");
    println!("\rScraping finito, salvataggio su file CSV...");

    let mut entries = entries.into_iter().collect::<Vec<_>>();
    entries.sort_by_key(|e| (e.name.to_lowercase(), e.address.to_lowercase()));
    entries.dedup_by(|a, b| a == b);

    // TODO: add separator option
    let mut csv_writer = csv::WriterBuilder::new()
        .flexible(false)
        .from_path(output_path)?;

    for entry in entries {
        csv_writer.serialize(entry)?;
    }
    csv_writer.flush()?;
    
    Ok(())
}
