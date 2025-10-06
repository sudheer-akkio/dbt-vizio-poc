# DBT Pipeline for Vizio POC

### Instructions

- Create venv
    ``` 
        python3 -m venv venv
        source venv/bin/activate
    ```
- `pip install dbt-core dbt-snowflake dbt-databricks`
- Make sure you have a `vizio_poc_databricks` profile in `~/.dbt/profiles.yml`
    - This should look like this: 
        ```yaml
        vizio_poc_databricks:
            outputs:
                dev:
                    type: databricks
                    host: "{{ env_var('DBT_DATABRICKS_HOST') }}"
                    http_path: "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}"
                    token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
                    catalog: akkio  # Writeable catalog for dbt outputs
                    schema: vizio_poc              # Schema for dbt models
                    threads: 4
            target: dev 
        ```
- To run the pipeline (after sourcing your venv)
    - `dbt run`

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices