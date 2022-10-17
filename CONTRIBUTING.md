Contributing to dbt-dremio
===================

Thank you for your interest in contributing to dbt-dremio.

This document summarizes the different ways you can contribute.

Code of conduct
-----------------------------------------------
You must agree to abide by the dbt-dremio [Code of Conduct](CODE_OF_CONDUCT.md).

Feature requests and bug reports
-----------------------------------------------

If you believe you have found an issue in dbt-dremio, or have an idea for a feature missing in our software, you might not be alone. Please start by searching https://github.com/dremio/dbt-dremio/issues and check if someone else has already reported it.

If you're the first one to report a bug, please include as much information as possible, so we can diagnose and possibly reproduce:

 - dbt-dremio version (`pip show dbt-dremio` - please try to reproduce the issue with the latest available version, as it might have already been fixed)
 - Dremio version, available under `Help > About Dremio`
 - dbt version, (`pip show dbt-core`)
 - Python version (`python -V`)
 - OS (`uname -a` output on Unix platforms, or equivalent for Windows)
 - Information about the query: the query itself, the query profile, the data sources, and any other information you can provide that might be helpful
 - If queried data is public, a link to it
 - Stacktrace from the logs

For a feature request, please create a new topic describing the feature, what it is supposed to do, and why you need it. If someone else already created a topic for it, please contribute to the discussion.

Contributing code and documentation changes
-------------------------------------------

Here are the recommended steps if you are ready to contribute code or a documentation change:

### Verify if someone else already proposed the same change

Check https://github.com/dremio/dbt-dremio/pulls to see if someone is already working on it.

### Fork and clone the repository

You will need to fork `dbt-dremio` repository and clone it to your local machine. See
[github help page](https://help.github.com/articles/fork-a-repo) for help.

Instructions on how to install dbt-dremio are available in the [README.md](README.md) file at the root of the repository.

### Open a pull request

Once your changes are ready to be submitted:

1. Sign the CLA (Contributor License Agreement)

    Please make sure you have signed our [Contributor License Agreement](https://www.dremio.com/legal/contributor-agreement/), which enables Dremio to distribute your contribution without restriction.

2. Submit a pull request

    Push your local changes to your forked copy of the repository and [submit a pull request](https://help.github.com/articles/using-pull-requests). Don't forget to choose a title for your change, and don't hesitate to add details in your description.

3. Reply to feedback and wait
  You will probably be contacted by a Dremio team member regarding your changes, with some questions and possibly some suggestions or change requests. If everything looks good (and CLA is signed), your contribution will be merged in our internal repository, and available in `dbt-dremio` repository with our next release.
