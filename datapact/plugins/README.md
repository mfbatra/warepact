# DataPact Community Plugins

Drop community adapter files here, or install them as packages:

```bash
pip install datapact-databricks
pip install datapact-teams
pip install datapact-dbt
```

Any installed package that registers adapters via `@PluginRegistry.register_*`
will be auto-discovered when `PluginRegistry.autodiscover()` runs.
