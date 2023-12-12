FormS is a project undergoing active development. This guide contains information on the workflow for contributing to the FormS codebase.


# Setting up Build and Installation Process

To setup FormS manually for development purposes, you should [fork](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo) the Github repo and clone the forked version.

```bash
git clone https://github.com/USERNAME/forms.git
```

You can install FormS using the editable mode:

```bash
cd forms/
pip install -e .
```

# Code Formatting
In order to keep our codebase clean and readible, we are using PEP8 guidelines. To help us maintain and check code style currently we use the following `pre-commit` hooks to automatically format the code on every commit and enforce its formatting on CI:

* [black](https://github.com/psf/black)

To automatically reformat the files, run 

```
make reformat
```

# Running the Test Suite

There is a suite of test files for ensuring that FormS is working correctly. You can run them locally to make sure that your changes are working and do not break any of the existing tests.

To run all the tests, including checking for code formatting, run:

```
make test
```

To run a single test file, run:

```
python -m pytest tests/<test_file_name>.py
```

# Submitting a Pull Request

You can commit your code and push to your forked repo. Once all of your local changes have been tested and formatted, you are ready to submit a PR. For FormS, we use the "Squash and Merge" strategy to merge in PR, which means that even if you make a lot of small commits in your PR, they will all get squashed into a single commit associated with the PR. Please make sure that comments and unnecessary file changes are not committed as part of the PR by looking at the "File Changes" diff view on the pull request page.
    
Once the pull request is submitted, the maintainer will get notified and review your pull request. They may ask for additional changes or comment on the PR. You can always make updates to your pull request after submitting it.
