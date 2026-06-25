---
title: Congress Press Search
emoji: 🏛️
colorFrom: indigo
colorTo: gray
sdk: docker
app_port: 7860
pinned: false
license: mit
---

# Congress Press Search

A [Datasette](https://datasette.io/) instance providing full-text + faceted search
over ~680k congressional press releases (2001–present).

This Space holds **only the serving layer**. The search database (`press.db`) is built
daily by the [dwillis/congress-press](https://github.com/dwillis/congress-press) GitHub
Action and published to the [`dpwillis/congress-press-db`](https://huggingface.co/datasets/dpwillis/congress-press-db)
dataset. On startup the container downloads the latest `press.db` and the latest
`metadata.yml` config, then serves them.

To refresh after a new build, the GitHub Action calls the HF "restart Space" API.

## Local run

```bash
docker build -t congress-press-search .
docker run -p 7860:7860 congress-press-search
```

Override the source dataset with `-e HF_DATASET=owner/repo`.
