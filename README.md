# tim: Tercen Intelligent Modules

## Installation

### From local repository

```
roxygen2::roxygenise()
devtools::install()
```

### From GitHub

```
devtools::install_github("tercen/tim", ref="0.0.13")
```

## Usage

### Developement workflow utilities

```
tim::set_workflow_step_ids(data_step_url)
```

```
tim::set_tercen_credentials()
```

### Test utilities

#### Populate test data

```
tim::build_test_data(out_table = out_table, ctx = ctx, test_name = "test1")
```

#### Check test data

```
tim::check_test_local(out_table = out_table, test_name = "test1")
```

### Operator folder utilities

#### Populate GitHub workflow files

```
tim::populate_gh_workflow(type = "R")
tim::populate_gh_workflow(type = "docker")
```
