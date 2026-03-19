# Repositories

- [Henry A. Murray Research Archive](https://www.murray.harvard.edu/)
Accessed via API

- [International Household Survey Network](https://ihsn.org/)

---

Install all dependencies:

```shell
uv sync
```

---

## Harvard API

- **[Search API Guide](https://guides.dataverse.org/en/latest/api/search.html):** This is for **Discovery**. You use this to find what exists (titles, IDs, and descriptions) based on keywords or collections.
- **[Data Access API Guide](https://guides.dataverse.org/en/latest/api/dataaccess.html):** This is for **Ingestion**. Once you have a file ID from the Search API, this tells you exactly how to download the actual bytes.
- **[Native API Guide](https://guides.dataverse.org/en/latest/api/native-api.html):** This is for **Metadata**. If you need the super-deep, technical XML/JSON "DDI" metadata (crucial for your future classification step), you’ll find it here.



```shell
uv run src/main.py 
```