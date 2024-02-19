# microServiceOrgNetwork

## Recreate the files

Clone the [eShopOnContainers](https://github.com/M3SOulu/eShopOnContainers) repo and save the path to `PATH_TO_REPO`.
To recreate all the commit tables, execute the commands:

```python
python mison.mison commit --commit_table "data/eshop_commits_200.csv" --import_mapping mappings/eshoponcontainers.py --repo $PATH_TO_REPO --from_commit 3cbaf4167498328f50ef2530f57071ac1bca1bb9 --to_tag 2.0
python mison.mison commit --commit_table "data/eshop_commits_205.csv" --import_mapping mappings/eshoponcontainers.py --repo $PATH_TO_REPO --from_tag 2.0 --to_tag v2.05
python mison.mison commit --commit_table "data/eshop_commits_206.csv" --import_mapping mappings/eshoponcontainers.py --repo $PATH_TO_REPO --from_tag v2.05 --to_tag 2.0.6
python mison.mison commit --commit_table "data/eshop_commits_207.csv" --import_mapping mappings/eshoponcontainers.py --repo $PATH_TO_REPO --from_tag 2.0.6 --to_tag 2.0.7
python mison.mison commit --commit_table "data/eshop_commits_208.csv" --import_mapping mappings/eshoponcontainers.py --repo $PATH_TO_REPO --from_tag 2.0.7 --to_tag 2.0.8
python mison.mison commit --commit_table "data/eshop_commits_220.csv" --import_mapping mappings/eshoponcontainers.py --repo $PATH_TO_REPO --from_tag 2.0.8 --to_tag 2.2.0
python mison.mison commit --commit_table "data/eshop_commits_221.csv" --import_mapping mappings/eshoponcontainers.py --repo $PATH_TO_REPO --from_tag 2.2.0 --to_tag 2.2.1
python mison.mison commit --commit_table "data/eshop_commits_300.csv" --import_mapping mappings/eshoponcontainers.py --repo $PATH_TO_REPO --from_tag 2.2.1 --to_tag 3.0.0
python mison.mison commit --commit_table "data/eshop_commits_310.csv" --import_mapping mappings/eshoponcontainers.py --repo $PATH_TO_REPO --from_tag 3.0.0 --to_tag 3.1.0
python mison.mison commit --commit_table "data/eshop_commits_311.csv" --import_mapping mappings/eshoponcontainers.py --repo $PATH_TO_REPO --from_tag 3.1.0 --to_tag 3.1.1
python mison.mison commit --commit_table "data/eshop_commits_500.csv" --import_mapping mappings/eshoponcontainers.py --repo $PATH_TO_REPO --from_tag 3.1.1 --to_tag 5.0.0
python mison.mison commit --commit_table "data/eshop_commits_600.csv" --import_mapping mappings/eshoponcontainers.py --repo $PATH_TO_REPO --from_tag 5.0.0 --to_tag 6.0.0
```

Note: commit tables for releases `2.0.7`, `2.2.1`, `3.0.0`, `3.1.1`, `5.0.0` will be empty and are ignored when
constructing the network