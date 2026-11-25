# Git workflow (Azure Repos)

- `main` – production, protected (PR + 1 reviewer + successful CI + linked work item)
- `develop` – integration, deploys to DEV automatically
- `feature/<AB#id>-short-name`, `bugfix/...`, `hotfix/...`

```bash
git checkout develop && git pull
git checkout -b feature/AB-123-add-supplier-scorecard
git add . && git commit -m "feat: add supplier scorecard gold table"
git push -u origin feature/AB-123-add-supplier-scorecard
# open PR -> develop; after UAT merge develop -> main via PR
```

Commit convention: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`.
Tag releases: `git tag -a v1.0.0 -m "First production release"`.
Notebooks are stored as `.py` source for clean diffs. Never commit secrets.
