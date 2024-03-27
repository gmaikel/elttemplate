# ELT Template

`elttemplate` is a project created using the [cookiecutter](https://cookiecutter.readthedocs.io/) library.

`elttemplate` allows you to focus on the most important tasks: creating your DAGs and your dbt models.

*Feel free to modify the code to adapt it to your usage.*

## Installation

```bash
conda create -n elt_dbt python=3.10 -y
conda activate elt_dbt
```

```bash
pip install cookiecutter
```

## Creating the ELT Template

```bash
cookiecutter git@github.com:gmaikel/elttemplate.git
```

Then configure your project:

![template configuration](docs/config.png)

Based on your choices, your ELT pipeline template for Airflow dbt will be created.

## Usage

Open `README.md` of your project template.

## Author
[Maikel G](https://github.com/gmaikel/)

