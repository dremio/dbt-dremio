<p align="center">
  <img src="https://resumo.cloud/wp-content/uploads/2021/07/modelo-imagem-rc-16-1.png" alt="dremio logo" width="500"/>
</p>

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-dremio version 1.4.5

The `dbt-dremio` package contains all of the code enabling dbt to work with [Dremio](https://www.dremio.com/). For more information on using dbt with Dremio, consult [the docs](https://docs.getdbt.com/reference/warehouse-profiles/dremio-profile).

The dbt-dremio package supports both Dremio Cloud and Dremio Software (versions 22.0 and later).

Version 1.4.5 of the dbt-dremio adapter is compatible with dbt-core versions 1.2.0 to 1.4.5.

> Prior to version 1.1.0b, dbt-dremio was created and maintained by [Fabrice Etanchaud](https://github.com/fabrice-etanchaud) on [their GitHub repo](https://github.com/fabrice-etanchaud/dbt-dremio). Code for using Dremio REST APIs was originally authored by [Ryan Murray](https://github.com/rymurr). Contributors in this repo are credited for laying the groundwork and maintaining the adapter till version 1.0.6.5. The dbt-dremio adapter is maintained and distributed by Dremio starting with version 1.1.0b.

## Getting started

-   [Install dbt-dremio](https://docs.getdbt.com/reference/warehouse-setups/dremio-setup)
    -   Version 1.4.5 of dbt-dremio requires dbt-core >= 1.2.0 and <= 1.4.5. Installing dbt-dremio will automatically upgrade existing dbt-core versions earlier than 1.2.0 to 1.4.5, or install dbt-core v1.4.5 if no version of dbt-core is found.
-   Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

## Join the dbt Community

-   Be part of the conversation in the [dbt Community Slack](http://community.getdbt.com/)
-   Read more on the [dbt Community Discourse](https://discourse.getdbt.com)

## Reporting bugs and contributing code

-   Open bugs and feature requests can be found at [dbt-dremio's GitHub issues](https://github.com/dremio/dbt-dremio/issues).
-   Want to report a bug or request a feature? Let us know by on [Slack](https://getdbt.slack.com/archives/C049G61TKBK), or opening [an issue](https://github.com/dremio/dbt-dremio/issues/new)
-   Want to help us build dbt-dremio? Check out the [Contributing Guide](https://github.com/dremio/dbt-dremio/blob/main/CONTRIBUTING.md).

## Code of Conduct

Everyone interacting in the dbt-dremio project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt-dremio Code of Conduct](https://github.com/dremio/dbt-dremio/blob/main/CODE_OF_CONDUCT.md).
