# Paginegialle Scraper
Simple scraper which collects business names, addresses and phones numbers from PagineGialle's search results.
Given the region, province, and business category, the scraper will for look businesses data in every province's city.
The parser is fast and parallel; expect a scraping execution time (average of a few thousands HTTP requests) below a minute.

# Usage
> [!NOTE]
> Be aware, if any of the parameters contains space, it should be replaced with underscores.
```bash
paginegialle-scraper -h
```

# Build
Requires the Rust toolchain. Prefer the release version, as it is way faster.
```bash
cargo build -r
```
