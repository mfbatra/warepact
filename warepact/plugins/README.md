# DataPact Community Plugins

Drop community adapter files here, or install them as packages:

```bash
pip install warepact-databricks
pip install warepact-teams
pip install warepact-dbt
```

Any installed package that registers adapters via `@PluginRegistry.register_*`
will be auto-discovered when `PluginRegistry.autodiscover()` runs.
