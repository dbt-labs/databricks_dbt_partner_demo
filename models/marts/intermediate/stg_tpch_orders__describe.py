def model(dbt, session):
    dbt.config(
        materialized = "table",
        create_notebook=True, # writes to /Shared/dbt_python_model/
        cluster_id="0408-145556-egmplhh0"
    )

    orders = dbt.ref("stg_tpch_orders")

    # describe the data
    described = orders.describe()

    return described