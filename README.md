# dbt + MSFabric Demo

This is a modified version of public [tutorial](https://docs.getdbt.com/tutorial/setting-up)
intended for users of dbt on MSFabric.

## Sample data

Create MSFabric tables `orders`, `customers`,
and `payments` from these CSV files, which are located in seeds folder

## Getting started

The instructions below assume you are running dbt on macOS. Linux and Windows 
users should adjust the bash commands accordingly.

1. Clone this github repo
2. Activate python virtual environment
3. Install [dbt-fabric](https://github.com/microsoft/dbt-fabric): `pip install dbt-fabric`
4. Go to your `~/.dbt` folder (created when installing dbt) and create profiles.yml:
5. Populate `~/.dbt/profiles.yml` with your MSFabric information (https://docs.getdbt.com/docs/core/connect-data-platform/fabric-setup), please use environment variable according to https://docs.getdbt.com/reference/dbt-jinja-functions/env_var
```bash
open ~/.dbt
```
6. Verify that you can connect to Databricks
```
$ dbt debug
```
7. Verify that you can run dbt
```
$ dbt run
```
8. Test the output of the models:
```
$ dbt test
```

9. Generate documentation for the project:
```bash
$ dbt docs generate
```

10. View the documentation for the project:
```bash
$ dbt docs serve
```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
- Check out the [reference](https://docs.getdbt.com/reference/references-overview)
