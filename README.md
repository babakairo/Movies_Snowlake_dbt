# Movies_Snowlake_dbt
Full end-to-end data engineering pipeline using TMDb movie data: - Python ingestion (src/) with TMDb API v3 - Bronze layer: raw JSON deduplication with row hashing - Silver layer: incremental typed models (movies, genres, companies) - Gold layer: star schema (fact_movies, dim_dates, dim_genres, dim_production_companies) 
