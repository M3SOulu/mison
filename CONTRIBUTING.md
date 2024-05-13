# Contributing guidelines

If you wish to submit a pull request with some contributions, the main rule is:

- Raise an issue first

This way, we can discuss what you are trying to add and whether it makes sense for the project. Do not send Pull Requests without warning!

# Contributing a backend

The main contributions we seek for MiSON are different commit mining backends. These can be different APIs or Python libraries interacting with `git` (or some other VCS!).

The checklist for contributing a backend to MiSON:

- Add a function `BACKEND_mine_commits` to `mison.mine`
  - The function should accept at least:
    - Repository to mine
    - Output filename (with the possibility to pass `default`, see how it is implemented for existing backends)
    - Function mining filenames to microservices
  - Other parameters are possible depending on the backend
  - The function should return a pandas DataFrame with the same data structure as other backends
- Add the following to `mison.__main__`
  - The choice of your backend for the `--backend` parameter
  - An argument group with all arguments specific to your backend
  - An `elif` statement to `main_commit` which calls your backend function correctly using arguments parsed to `args`
- Update the wikis and README.md accordingly
