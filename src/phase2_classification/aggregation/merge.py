from mapping import normalize_url, normalize_doi, normalize_title, normalize_text


class MergedProject:
    def __init__(self, fields):
        self.fields = dict(fields)
        self.files = {}
        self.keywords = {}
        self.person_role = {}
        self.licenses = {}

    def coalesce_fields(self, fields):
        for key, value in fields.items():
            if self.fields.get(key) is None and value is not None:
                self.fields[key] = value

    def add_file(self, record):
        key = normalize_text(record.get("file_name"))
        if key is None:
            return
        existing = self.files.get(key)
        if existing is None:
            self.files[key] = dict(record)
        else:
            for field, value in record.items():
                if existing.get(field) is None and value is not None:
                    existing[field] = value

    def add_keyword(self, keyword):
        key = normalize_text(keyword)
        if key is None:
            return
        self.keywords.setdefault(key, keyword)

    def add_person_role(self, name, role):
        key = (normalize_text(name), normalize_text(role))
        if key == (None, None):
            return
        self.person_role.setdefault(key, (name, role))

    def add_license(self, license_text):
        key = normalize_text(license_text)
        if key is None:
            return
        self.licenses.setdefault(key, license_text)


class Aggregator:
    def __init__(self):
        self.projects = []
        self.url_index = {}
        self.doi_index = {}
        self.title_repo_index = {}

    def _find_existing(self, fields):
        norm_url = normalize_url(fields.get("project_url"))
        if norm_url is not None and norm_url in self.url_index:
            return self.url_index[norm_url]

        norm_doi = normalize_doi(fields.get("doi"))
        if norm_doi is not None and norm_doi in self.doi_index:
            return self.doi_index[norm_doi]

        norm_title = normalize_title(fields.get("title"))
        repository_id = fields.get("repository_id")
        if norm_title is not None and repository_id is not None:
            key = (norm_title, repository_id)
            if key in self.title_repo_index:
                return self.title_repo_index[key]

        return None

    def _index(self, merged_index, fields):
        norm_url = normalize_url(fields.get("project_url"))
        if norm_url is not None:
            self.url_index.setdefault(norm_url, merged_index)

        norm_doi = normalize_doi(fields.get("doi"))
        if norm_doi is not None:
            self.doi_index.setdefault(norm_doi, merged_index)

        norm_title = normalize_title(fields.get("title"))
        repository_id = fields.get("repository_id")
        if norm_title is not None and repository_id is not None:
            self.title_repo_index.setdefault((norm_title, repository_id), merged_index)

    def upsert_project(self, fields):
        merged_index = self._find_existing(fields)
        if merged_index is None:
            merged_index = len(self.projects)
            self.projects.append(MergedProject(fields))
        else:
            self.projects[merged_index].coalesce_fields(fields)

        self._index(merged_index, self.projects[merged_index].fields)
        return merged_index

    def add_file(self, merged_index, record):
        self.projects[merged_index].add_file(record)

    def add_keyword(self, merged_index, keyword):
        self.projects[merged_index].add_keyword(keyword)

    def add_person_role(self, merged_index, name, role):
        self.projects[merged_index].add_person_role(name, role)

    def add_license(self, merged_index, license_text):
        self.projects[merged_index].add_license(license_text)
