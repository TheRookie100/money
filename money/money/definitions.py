from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets

# Carrega todos os assets do módulo 'assets'
all_assets = load_assets_from_modules([assets])

# Defina o job que materializará os assets
bcb_exchange_rates_job = define_asset_job(
    "bcb_exchange_rates_job",
    selection=AssetSelection.keys(
        "read_currency_pairs",
        "extract_exchange_rates",
        "save_exchange_rates",
        "analyze_exchange_rates"
    )
)

# Defina a ScheduleDefinition para o job - executa diariamente às 8h
bcb_daily_schedule = ScheduleDefinition(
    job=bcb_exchange_rates_job,
    cron_schedule="0 8 * * *",  # Às 8h de cada dia
)

# Definição do repositório de assets
defs = Definitions(
    assets=all_assets,
    schedules=[bcb_daily_schedule],
)

