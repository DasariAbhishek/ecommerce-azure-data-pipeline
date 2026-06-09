# How to push this project to your GitHub

I can't connect to GitHub directly, so here are the exact steps. Takes ~3 minutes.

## 1. Create an empty repo on GitHub
Go to https://github.com/new and create a repo named:

    ecommerce-azure-data-pipeline

Leave "Add a README / .gitignore / license" **unchecked** (this project already has them).

## 2. Open a terminal in the project folder
`cd` into the folder where this project lives (the one containing `README.md`).

> Before pushing, optionally delete the local `data/` outputs — they are
> regenerated on every run and are already git-ignored:
> `rm -rf data/lakehouse data/landing/*.csv data/landing/*.json` (mac/linux)
> or just delete the `data/lakehouse` folder in Explorer (Windows).

## 3. Push it

Replace `YOUR_USERNAME` with your GitHub username.

```bash
git init
git add .
git commit -m "feat: end-to-end e-commerce medallion data pipeline (Spark + Azure)"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/ecommerce-azure-data-pipeline.git
git push -u origin main
```

If git asks for a password, use a **Personal Access Token** (not your GitHub
password): GitHub → Settings → Developer settings → Personal access tokens →
Tokens (classic) → Generate new token with `repo` scope, then paste it as the
password.

## 4. (Optional) verify CI ran
After pushing, open the **Actions** tab on your repo — the CI workflow
(`.github/workflows/ci.yml`) will run the tests and a smoke run automatically.

## 5. Polish for the LinkedIn post
- Add a short repo description and topics: `data-engineering`, `azure`,
  `pyspark`, `databricks`, `etl`, `medallion-architecture`.
- The README already embeds the architecture diagram and a quick-start.
- Export `docs/architecture.svg` to PNG to attach to your LinkedIn post
  (open the SVG in a browser → screenshot, or use any SVG-to-PNG tool).

That's it — your repo link goes into the LinkedIn post (see `LINKEDIN_POST.md`).
