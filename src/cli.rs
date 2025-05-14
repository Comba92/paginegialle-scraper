use crate::DEFAULT_PAGE_LIMIT;

#[derive(clap::Parser)]
#[command(version, about = "Scrapes PagineGialle businesses data into a csv file. Puntuactions should be replaced with _")]
pub struct Cli {
  /// which kind of search to request
  #[command(subcommand)]
  pub mode: CliMode,

  /// output filename (without the .csv extension)
  #[arg(short, long = "output", default_value = "output")]
  pub output_file: String,

  /// maximum pages to be scraped for each query
  #[arg(short = 'l', long = "limit", default_value_t = DEFAULT_PAGE_LIMIT)]
  pub page_limit: usize,

  /// show debugging info
  #[arg(short, long)]
  pub debug: bool,
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
pub enum CliMode {
    /// Needs a search query, and optionally a city.
    /// (urls of kind `https://www.paginegialle.it/<regione>/<citta>/<categoria>.html`)
    Search(SearchMode),
    /// Needs a region name, and optionally a city and a category.
    /// (urls of kind `https://www.paginegialle.it/ricerca/<search>/[<citta>]`)
    Filter(FilterMode),
    /// Merges computed CSVs files into a single one, removing duplicates.
    Merge(MergeMode),
}

#[derive(clap::Args)]
pub struct SearchMode {
    /// search query, should be a business category or a business name
    pub query: String,
    /// location to search businesses in (might be city or region)
    pub location: Option<String>,
}

#[derive(clap::Args)]
pub struct FilterMode {
    /// region to search businesses in
    pub region: String,

    /// city to search businesses in.
    /// If left empty, will scrape for ALL cities in the region
    pub city: Option<String>,

    #[arg(short, long)]
    /// business category to search for
    pub category: Option<String>,

    #[arg(short, long)]
    /// if city provided is a province (example: Padova), setting this flag will scrape all cities in the province.
    /// If city is not a region or province, this flag does nothing
    pub all_regions_cities: bool,

    #[arg(short, long)]
    /// halven the cities list if scarping for whole regions or provinces, this will make the process faster (less requests) but will give less result.
    /// If parsing only for a single city, this flag does nothing
    pub big_cities_only: bool,
}

#[derive(clap::Args)]
pub struct MergeMode {
    /// target CSVs folder to merge in one. Only CSVs files will be selected
    pub folder_path: String,   
}