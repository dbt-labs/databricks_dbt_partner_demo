def model(dbt, session):
    dbt.config(
        materialized="table",
        create_notebook=False, # True writes to /Shared/dbt_python_model/
        cluster_id="0830-163228-q22s4sv5"
    )

    orders = dbt.ref("stg_tpch_orders")

    # describe the data
    described = orders.describe()

    return described
