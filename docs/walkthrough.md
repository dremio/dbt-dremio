# Using dbt with Dremio Walkthrough

- [Dremio Docs](https://docs.dremio.com)
- [Dremio Semantic Layer CI/CD with dbt whitepaper](https://www.dremio.com/wp-content/uploads/2024/01/Semantic-Layer-CI_CD-with-Dremio-and-dbt.pdf)
- [dbt-dremio adapter Repository](https://github.com/dremio/dbt-dremio)

## Table of Contents

- [Starting Up Dremio on Your Laptop](#starting-up-dremio-on-your-laptop)
- [Creating a Python Virtual Environment and Installing dbt](#creating-a-python-virtual-environment-and-installing-dbt)
- [Creating Your dbt Project](#creating-your-dbt-project)
  - [`dbt init` prompts](#dbt-init-prompts)
    - [Basics](#basics)
    - [Connection Settings and Authentication](#connection-settings-and-authentication)
    - [Default View and Table Storage Location](#9-12-default-view-and-table-storage-location)
      - [Materializing Tables Location](#materializing-tables-location)
      - [Creating Views Location](#creating-views-location)
    - [Threads](#13-thread)
- [Customizing Configurations in dbt with Dremio](#customizing-configurations-in-dbt-with-dremio)
  - [Configuring Defaults for a Group of Models in `dbt_project.yml`](#configuring-defaults-for-a-group-of-models-in-dbt_projectyml)
  - [Configuring an Individual Model with the config Function](#configuring-an-individual-model-with-the-config-function)
  - [Explanation of Key Configuration Options](#explanation-of-key-configuration-options)
- [Common dbt Commands and Their Purpose](#common-dbt-commands-and-their-purpose)
  - [Core dbt Commands](#core-dbt-commands)
  - [Tips for Using dbt Commands](#tips-for-using-dbt-commands)
- [Writing a `schema.yml` File in dbt](#writing-a-schemayml-file-in-dbt)
  - [Where Should `schema.yml` Files Be Located?](#where-should-schemayml-files-be-located)
  - [Structure of a `schema.yml` File](#structure-of-a-schemayml-file)
  - [Key Properties in schema.yml](#key-properties-in-schemayml)
  - [Best Practices for schema.yml Files](#best-practices-for-schemayml-files)
- [Generating Documentation with dbt-Dremio: Syncing Descriptions and Tags](#generating-documentation-with-dbt-dremio-syncing-descriptions-and-tags)
  - [Enabling the Sync](#enabling-the-sync)
  - [How It Works](#how-it-works)
  - [Example schema.yml File](#example-schemayml-file)
  - [What Happens in Dremio](#what-happens-in-dremio)
  - [Benefits of Enabling Persisted Documentation](#benefits-of-enabling-persisted-documentation)
- [Using Environment Variables in dbt](#using-environment-variables-in-dbt)
  - [Where to Use Environment Variables in dbt](#where-to-use-environment-variables-in-dbt)
  - [How to Define Environment Variables](#how-to-define-environment-variables)
    - [Inline with a Command](#inline-with-a-command)
    - [Pre-Defining in the Environment](#pre-defining-in-the-environment)
    - [Using a `.env` File](#using-a-env-file)
      - [Loading a .env file into the active shell](#loading-a-env-file-into-the-active-shell)
  - [Best Practices for Using Environment Variables](#best-practices-for-using-environment-variables)
- [Further Reading](#further-reading)


Unified Analytics with Dremio unlocks the ability to streamline your data workflows by providing a single, cohesive platform for querying and analyzing data across multiple sources. By integrating dbt into your Dremio semantic layer, you can elevate your data modeling process to new heights. Dremio's dbt integration not only orchestrates SQL workflows but also enables seamless synchronization of documentation and tags directly with Dremio, ensuring that your data assets are well-organized and discoverable. With the ability to run tests, model across various Dremio-supported data sources, and define incremental pipelines, you can build robust and scalable analytics processes. Moreover, this integration allows you to define and optimize reflections directly in your dbt models, accelerating query performance while maintaining consistency and governance across your datasets. This combination of Dremio and dbt creates a powerful toolkit for curating a unified, efficient, and high-performance analytics environment.

## Starting Up Dremio on Your Laptop

If you just need Dremio running on your laptop for this walkthrough run the docker command below:

```
docker run -p 9047:9047 -p 31010:31010 -p 45678:45678 -p 32010:32010 -e DREMIO_JAVA_SERVER_EXTRA_OPTS=-Dpaths.dist=file:///opt/dremio/data/dist --name try-dremio dremio/dremio-oss
```

If you want to run Dremio along with other data infrastructure for more complex locally run data lakehouse environments, check out [this repository of docker-compose files](https://github.com/developer-advocacy-dremio/dremio-compose).

Once Dremio is running in docker head over to localhost:9047 in your browser and create your username and password and take note of them. Then add the "Sample Source" which gives you access to data sets you can experiment with. Also create an initial space, spaces are top level storage for folders and views.

## Creating a Python Virtual Environment and Installing dbt

You should always isolate your python dependencies by creating a virtual environment for each project.

Command to create the environment:

```
python -m venv venv
```

This will create a `venv` folder with your virtual environment we want to run the activate script in that folder:

- Linux/Mac `source venv/bin/activate`
- Windows (CMD) `venv\Scripts\activate`
- Windows (powershell) `.\venv\Scripts\Activate.ps1`

This script will update the `pip` and `python` command in your active shell to use the virtual environment not your global python environment.

Now we can install `dbt-dremio` and other libraries you'd want to use.

`pip install dbt-dremio`

## Creating Your dbt Project

Now we can create a new dbt project using the command `dbt init`, this will be begin a series of questions to setup the global configurations of your project, these configurations are stored in `~/.dbt/profiles.yml`. You can setup more localized configurations in your project as we'll discuss shortly.

### `dbt init` prompts

#### Basics
1. *name of your project*: enter whatever name you like
2. *number of type of project*: enter the number for `dremio` (may not be 1 if you have other dbt libraries installed)
3. *type of dremio project*: For this exercise choose `2` (The Following questions will differ depending on what you select for this option)
    - [1] dremio_cloud (if you are using Dremio Cloud, you'll need a Personal Access Token generated from the UI or REST API)
    - [2] software_with_username_password (if you are self-deploying Dremio like using Docker and want to authenticate using Username/Password)
    - [3] software_with_pat (if you are self-deploying Dremio like using Docker and want to authenticate using Personal Access Token which must be generated with Dremio REST API)

#### Connection Settings and Authentication
4. *software host*: The host part of the url for your Dremio instance, if running on your laptop this would be `localhost` or `127.0.0.1`
5. *port*: The port Dremio is running on within the host, which should typically be port `9047`
6. *username*: Your Dremio Username
7. *password*: Your Dremio Password
8. *use_ssl*: Whether to use a SSL connection (choose False if working locally)

#### 9-12: Default View and Table Storage Location

###### Materializing Tables Location

- `object_storage_source`: must be a Dremio source where tables can be written (S3, GCP, HDFS, AWS Glue, Polaris, Nessie), if you don't have one of these sources connected to Dremio accept the default `$scratch` option.
- `object_storage_path`: the sub path for your object_storage_source

These two settings establish where physical tables are created by default, so if I want to create tables by default in `nessie.marketing.bronze` then the values would be:

- `object_storage_source`: `nessie`
- `object_storage_path`: `marketing.bronze`

###### Creating Views Location

- `dremio_space`: This can be any Dremio Source that can track views (Spaces, Arctic Catalog, Nessie, Dremio Catalog) if you don't have any of these select the default option which will use the users homespace (every user gets a "home" space named after their username on Dremio Self-Deployed)

- `dremio_space_folder`: This the sub path for the dremio_space

If I want views to be create by a default in `default.views` then the values would be

- `dremio_space`: `default`
- `dremio_space_folder`: `views`

#### 13 Thread

Just select the default `1` thread unless you want to use more threads.

## Customizing Configurations in dbt with Dremio

When working with dbt and Dremio, you can override the default configurations for materialized tables and views at both the project level using the `dbt_project.yml` file and the model level using the `config` function within your model file. This flexibility allows you to tailor the storage locations for your tables and views based on your specific needs.

#### Configuring Defaults for a Group of Models in `dbt_project.yml`

To configure a group of models, update the `dbt_project.yml` file under the `models` section. For example, if you want to store certain tables by default in `nessie.marketing.bronze` and certain views in `default.views`, you can specify the following configuration:

```yaml
models:
  my_project:
    +object_storage_source: nessie
    +object_storage_path: marketing.bronze
    +dremio_space: default
    +dremio_space_folder: views
```
This configuration ensures that all models under the `models/my_project` folder will store materialized tables in the `nessie.marketing.bronze` path and create views in the `default.views` folder unless overridden at the individual model level.

#### Configuring an Individual Model with the config Function
To set custom configurations for a specific model, you can use the config function directly within the model file. For example, if you want to store the output of a specific model in the nessie.sales.silver path for tables and in the default.custom_views folder for views, you can include the following in your model SQL file:

```sql
{{ config(
    object_storage_source='nessie',
    object_storage_path='sales.silver',
    dremio_space='default',
    dremio_space_folder='custom_views'
) }}


SELECT *
FROM source_table
```

#### Explanation of Key Configuration Options

- `object_storage_source:` Defines the Dremio source where physical tables are created (e.g., nessie, S3, Polaris). If none of these are available, the default $scratch option is used.
- `object_storage_path:` Specifies the subpath within the object_storage_source for materialized tables.
- `dremio_space:` Sets the Dremio source for views (e.g., Spaces, Arctic Catalog, Nessie).
- `dremio_space_folder:` Defines the folder within the dremio_space for creating views.

By customizing these configurations, you can effectively organize your tables and views in Dremio to align with your data modeling and organizational standards.

## Common dbt Commands and Their Purpose

To effectively use dbt with Dremio, it’s important to familiarize yourself with the key dbt commands. **Before running any dbt command, ensure that your terminal is navigated to the folder containing your `dbt_project.yml` file.** This ensures dbt has access to the project configuration and can execute the commands successfully.

#### **Core dbt Commands**

1. **`dbt run`**
   - **Purpose**: Executes all the models defined in your project by running the SQL files and materializing the results (tables, views, or incremental models) according to the configurations.
   - **Use Case**: Use this command to build or refresh your data models in the target database.

   ```bash
   dbt run
   ```

2. **`dbt build`**

- **Purpose**: Combines multiple actions—compiling, running, testing, and building documentation—all in one command.
- **Use Case**: Use this for an end-to-end execution of models, ensuring they run, pass tests, and have documentation updated.
    
    ```
    dbt build
    ```

3. **`dbt test`**

- **Purpose**: Runs the tests defined in your tests directory or within your models to validate data quality and integrity.
- **Use Case**: Use this command to check that your data meets the defined constraints (e.g., unique values, no nulls).

    ```
    dbt test
    ```

4. **`dbt compile`**   
    
    ```
    dbt compile
    ```

- **Purpose**: Compiles all the SQL files in your project into executable SQL queries but does not execute them.
- **Use Case**: Use this command to validate the SQL generated by dbt without actually running it.

    ```
    dbt compile
    ```

5. **`dbt docs generate`**

- **Purpose**: Generates HTML-based documentation for your project, including model relationships, dependencies, and metadata.
- **Use Case**: Use this to create a navigable interface for understanding your data models.

    ```
    dbt docs generate
    ```

6. **`dbt docs serve`**

- **Purpose**: Serves the documentation generated by dbt docs generate on a local web server.
- **Use Case**: Use this to view and interact with your project documentation in a browser.

    ```
    dbt docs serve
    ```

7. **`dbt debug`**

- **Purpose**: Verifies that your dbt setup and configurations are correct and that dbt can connect to the target database.
- **Use Case**: Use this command to troubleshoot connection or configuration issues.

    ```
    dbt debug
    ```

8. **`dbt clean`**

- **Purpose**: Removes any artifacts created by dbt, such as compiled files or cached files in the target/ directory.
- **Use Case**: Use this command to reset the environment if you encounter unexpected behavior.

    ```
    dbt clean
    ```

9. **`dbt seed`**

- **Purpose**: Loads CSV files from the data/ directory into your database as tables.
- **Use Case**: Use this command to prepare static or reference data for your project.

    ```
    dbt seed
    ```

10. **`dbt snapshot`**

- **Purpose**: Executes snapshot models to capture the state of your data at specific points in time.
- **Use Case**: Use this to maintain historical data changes over time.

    ```
    dbt snapshot
    ```

11. **`dbt list`**

- **Purpose**: Lists the resources (models, tests, seeds, etc.) in your project.
- **Use Case**: Use this to get a quick overview of the components in your dbt project.

    ```
    dbt list
    ```

#### Tips for Using dbt Commands

- Commands like `dbt run`, `dbt test`, and `dbt build` can be scoped to specific models or directories by using selectors (e.g., `dbt run --select model_name`).
- Use `dbt clean` and `dbt debug` to ensure a smooth experience when switching between environments or debugging issues.
- Regularly generate and serve documentation with `dbt docs` to maintain an up-to-date understanding of your project.

By mastering these commands, you’ll be equipped to manage, test, and document your dbt project efficiently!

_note: not all of these commands may yet be supported by Dremio's dbt integration_

## Writing a `schema.yml` File in dbt

The `schema.yml` file in dbt is a key component of your project that defines metadata, tests, and documentation for your models, seeds, and snapshots. It provides a centralized place to describe your data and enforce quality standards, making your dbt project easier to manage and understand.

#### **Where Should `schema.yml` Files Be Located?**

- Place your `schema.yml` files in the same directory as the corresponding models they describe. This ensures that the metadata and tests are logically grouped with the resources they apply to.
- For example:
`models/ analytics/ orders.sql customers.sql schema.yml`

#### **Structure of a `schema.yml` File**

Below is an example structure of a `schema.yml` file, followed by explanations of its key components:

```yaml
version: 2

models:
- name: orders
  description: "Contains data about customer orders."
  columns:
    - name: order_id
      description: "Unique identifier for each order."
      tests:
        - unique
        - not_null
    - name: customer_id
      description: "References the ID of the customer who placed the order."
      tests:
        - not_null

- name: customers
  description: "Contains data about customers."
  columns:
    - name: customer_id
      description: "Unique identifier for each customer."
      tests:
        - unique
        - not_null
    - name: customer_name
      description: "The name of the customer."

seeds:
- name: countries
  description: "Static dataset containing country codes and names."
  columns:
    - name: country_code
      description: "Two-letter ISO country code."
      tests:
        - unique
        - not_null
    - name: country_name
      description: "Full name of the country."

snapshots:
- name: order_snapshots
  description: "Captures historical data about orders over time."
```
#### Key Properties in schema.yml

1. `version`

- **Purpose**: Specifies the version of the schema.yml file format being used. Always set this to 2.

2. `models`

- **Purpose**: Defines metadata, descriptions, and tests for the models in the current directory.

**Key Properties for `models`**:
- name: The name of the model (matches the .sql file name).
- description: A short explanation of the model's purpose or content.
- columns: Provides metadata and tests for each column in the model.

3. `columns`

- **Purpose**: Adds metadata and defines tests for individual columns.

**Key Properties for `columns`:**
- name: The column name in the model.
- description: A brief description of what the column represents.
- tests: A list of tests to validate the column (e.g., unique, not_null, or custom tests).

4. `seeds`

- **Purpose**: Provides metadata and tests for seed files (CSV files) located in the data/ directory.
- **Structure**: Similar to models, with name, description, and columns.

5. `snapshots`

- **Purpose**: Describes and documents snapshots that capture historical data.

**Key Properties for snapshots**:

- name: The name of the snapshot.
- description: A short explanation of the snapshot's purpose.

#### Best Practices for schema.yml Files
- Keep descriptions concise and informative. Use them to help others understand the purpose of each model and column.
- Define tests wherever possible. This ensures that your data meets quality standards.
- Organize by context. Place schema.yml files in directories that match the purpose or domain of the models they describe.

By maintaining well-documented and organized schema.yml files, you can ensure that your dbt project is not only functional but also easy to understand, debug, and scale.

## Generating Documentation with dbt-Dremio: Syncing Descriptions and Tags

When using `dbt-dremio`, you can seamlessly sync model descriptions and tags from your `dbt` project into Dremio as wikis and labels. This feature enhances your data documentation by making it accessible directly within Dremio's user interface, ensuring consistent metadata across tools.

#### **Enabling the Sync**

To enable this feature, you need to turn on the `persist_docs` property in your `dbt_project.yml` file with the `relation` option set to `True`. This configuration ensures that the model descriptions and tags are persisted in Dremio.

Example `dbt_project.yml` configuration:

```yaml
models:
  project1:
    example:
      +materialized: view
      +dremio_space: "{{ env_var('DBT_ENV', 'development') }}"
      +persist_docs:
        relation: True
```

#### How It Works

**Descriptions:**

The `description` property in the `schema.yml` file for a model is added to the model's wiki in Dremio.
This provides a central place for users to understand the purpose and structure of each model directly in Dremio.

**Tags:**

The tags defined in the `schema.yml` file within the model's config are converted into labels in Dremio.
These labels help organize and filter models within Dremio based on shared attributes or business domains.

#### Example schema.yml File
Here’s an example of how to define descriptions and tags for your models in `schema.yml`:

```yaml
version: 2

models:
  - name: my_first_dbt_model
    description: "A starter dbt model"
    columns:
      - name: id
        description: "The primary key for this table"
        data_tests:
          - unique
          - not_null
    tags: 
      - cheese
    config:
      tags: 
        - test

  - name: my_second_dbt_model
    description: "A starter dbt model"
    columns:
      - name: id
        description: "The primary key for this table"
        data_tests:
          - unique
          - not_null
    tags:
      - test
      - cheese
    config:
      tags: 
        - test
```

#### What Happens in Dremio

**Wiki Sync:**

- The description of `my_first_dbt_model` ("A starter dbt model") is added to the wiki of the corresponding view in Dremio.

**Label Sync:**

- The `test` tag is added as a label to the corresponding views in Dremio.
- tags listed under `config: tags:` are considered.

#### Benefits of Enabling Persisted Documentation
- **Centralized Metadata:** Descriptions and tags are automatically synchronized, reducing duplication of effort.
- **Improved Discoverability:** Labels in Dremio make it easier to search, filter, and categorize views.
- **Enhanced Collaboration:** With wikis synced, teams can quickly understand the context and purpose of models within Dremio.
By enabling persist_docs in your dbt_project.yml and leveraging descriptions and tags in your schema.yml, you can ensure that your Dremio environment is always enriched with up-to-date and meaningful documentation.

## Using Environment Variables in dbt

Environment variables are a powerful tool in dbt for managing dynamic configurations, securing sensitive information, and adapting to different environments (e.g., development, staging, production). They can be used in various places within dbt, including `profiles.yml`, `dbt_project.yml`, and individual models. Here’s how you can effectively use and define them.

#### **Where to Use Environment Variables in dbt**

1. **In `profiles.yml`**
   - Use environment variables to dynamically configure sensitive information like database credentials or schemas.
   - Example:
     ```yaml
           dremio_space: "{{ env_var('DBT_SPACE', 'default_catalog') }}"
           dremio_space_folder: "{{ env_var('DBT_ENV', 'development') }}" 
     ```

2. **In `dbt_project.yml`**
   - Use environment variables to configure model settings, tags, or storage paths dynamically.
   - Example:
     ```yaml
     models:
       sales_data_product:
         dremio_space: "{{ env_var('DBT_SPACE', 'default_catalog') }}"
         dremio_space_folder: "sales.{{ env_var('DBT_ENV', 'development') }}" 
     ```

3. **In Individual Models**
   - Use environment variables within a `config` call to parameterize table storage or view paths.
   - Example:
     ```sql
     {{ config(
         materialized='view',
         dremio_space=env_var('DBT_SPACE', 'default_catalog'),
         dremio_space_folder=f`sales.{env_var('DBT_ENV', 'development')}`
     ) }}

     SELECT *
     FROM source_table
     ```

---

#### **How to Define Environment Variables**

1. **Inline with a Command**
   - Pass environment variables directly when running dbt commands:
     ```bash
     DBT_SPACE=partner_catalog DBT_ENV=production dbt run
     ```

    ```powershell
    $env:DBT_ENV = “production”; dbt run
    ```

   - This method is useful for ad-hoc executions or temporary overrides.

2. **Pre-Defining in the Environment**
   - Set the environment variables globally in your terminal or operating system:
     - **Linux/Mac**:
       ```bash
       export DBT_SPACE=partner_catalog
       export DBT_ENV=production
       ```
     - **Windows (Command Prompt)**:
       ```cmd
       set DBT_SPACE=partner_catalog
       set DBT_ENV=production
       ```
     - **Windows (PowerShell)**:
       ```powershell
       $env:DBT_SPACE = "partner_catalog"
       $env:DBT_ENV = "production"
       ```
   - This method is best for CI/CD pipelines or when working on a specific session.

3. **Using a `.env` File**
   - Create a `.env` file to store your environment variables, and use a library like `python-dotenv` (if applicable) to load them.
   - Example `.env` file:
     ```
     DBT_SPACE=partner_catalog
     DBT_ENV=production
     ```
   - To load the `.env` file in dbt, set up your environment or ensure your deployment pipeline supports loading it.

#### Loading a .env file into the active shell

**Bash (Linux/Mac)**
To load the .env file and make its variables available in your current session:

```bash
set -o allexport && source .env && set +o allexport
```
**Explanation**:
- `set -o allexport`: Automatically exports all variables loaded into the shell.
- `source .env`: Reads and executes the .env file to load the variables.
- `set +o allexport`: Turns off automatic export to prevent overwriting variables unintentionally.

Alternatively, for simpler cases:

```bash
export $(grep -v '^#' .env | xargs)
```

**Command Prompt (Windows CMD)**
Command Prompt does not have a built-in way to load .env files, but you can simulate it by manually setting the variables. If the file has many variables, you can write a script or use a tool like `dotenv-cli`.

For a single variable:

```cmd
for /F "tokens=1,2 delims==" %A in ('.env') do set %A=%B
```

**Recommended Tool:**
Use a tool like `dotenv-cli`:

Install via npm:
```cmd
npm install -g dotenv-cli
```

Run your commands with the .env file loaded:
```cmd
dotenv dbt run
```

**PowerShell**
To load a `.env` file in PowerShell:

```powershell
Get-Content .env | ForEach-Object {
    if ($_ -notmatch "^\s*#") {
        $name, $value = $_ -split '=', 2
        $env:$name = $value
    }
}
```

Explanation:
- `Get-Content .env`: Reads the .env file line by line.
- `if ($_ -notmatch "^\s*#")`: Skips lines starting with # (comments).
- `$name, $value = $_ -split '=', 2`: Splits each line into key and value.
- `$env:$name = $value`: Sets the environment variable for the current session.


#### **Best Practices for Using Environment Variables**
- **Secure Sensitive Information**: Never hardcode credentials or secrets in your `dbt_project.yml` or any files checked into git. Use environment variables instead.
- **Provide Defaults**: Always include a fallback value in `env_var()` to avoid errors when a variable isn’t set.
- **Document Variables**: Maintain a list of required environment variables for your team in a README or setup guide.
- **Adapt to Multiple Environments**: Use environment variables to switch between development, staging, and production seamlessly.

By leveraging environment variables effectively, you can make your dbt projects more dynamic, secure, and adaptable to any environment.

## Further Reading
- [dbt-dremio materialization and incremental features](https://github.com/dremio/dbt-dremio/wiki/Using-Materializations-with-Dremio)
- [Using Github Actions with dbt-dremio](https://www.dremio.com/blog/automating-dremio-dbt-with-github-actions/)
