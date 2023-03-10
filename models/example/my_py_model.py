def model(dbt, session):
    dbt.config(
        submission_method="all_purpose_cluster",
        create_notebook=False,
        cluster_id="0824-102659-hzdr9bht",
    )

    # get upstream data
    orders = dbt.ref("dim_customers")

    # describe the data
    described = orders.describe()

    return described