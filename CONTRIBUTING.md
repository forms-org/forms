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
# Commit Guidelines

## Commit Message formatting

To ensure that all commit messages in the main branch follow a specific format, we
enforce that all commit messages must follow the following format:

.. code-block:: bash

    FEAT-#9999: Add new functionality for feature XYZ

The ``FEAT`` component represents the type of commit. This component of the commit
message can be one of the following:

* FEAT: A new feature that is added
* DOCS: Documentation improvements or updates
* FIX: A bugfix contribution
* REFACTOR: Moving or removing code without change in functionality
* TEST: Test updates or improvements

The ``#9999`` component of the commit message should be the issue number in the 
GitHub issue tracker: https://github.com/totemtang/forms/issues. This is important
because it links commits to their issues.

The commit message should follow a colon (:) and be descriptive and succinct.

## Sign-off Procedure

To keep a clear track of who did what, we use a `sign-off` procedure on patches or pull requests that are being sent. This signed-off process is the same procedures used by many open-source projects, including the [Linux kernel](https://www.kernel.org/doc/html/v4.17/process/submitting-patches.html). The sign-off is a simple line at the end of the explanation
for the patch, which certifies that you wrote it or otherwise have the right to pass it
on as an open-source patch. If you can certify the below:

```
CERTIFICATE OF ORIGIN V 1.1
By making a contribution to this project, I certify that:

1.) The contribution was created in whole or in part by me and I have the right to
submit it under the open source license indicated in the file; or
2.) The contribution is based upon previous work that, to the best of my knowledge, is
covered under an appropriate open source license and I have the right under that license
to submit that work with modifications, whether created in whole or in part by me, under
the same open source license (unless I am permitted to submit under a different
license), as indicated in the file; or
3.) The contribution was provided directly to me by some other person who certified (a),
(b) or (c) and I have not modified it.
4.) I understand and agree that this project and the contribution are public and that a
record of the contribution (including all personal information I submit with it,
including my sign-off) is maintained indefinitely and may be redistributed consistent
with this project or the open source license(s) involved.
```

then you can add the signoff line at the end of your commit as follows: 

```bash
This is my commit message

Signed-off-by: Awesome Developer <developer@example.org>
```

Code without a proper signoff cannot be merged into the
master branch. You must use your real name and working email in the commit signature.

The signoff line can either be manually added to your commit body, or you can add either ``-s``
or ``--signoff`` to your usual ``git commit`` commands:

```bash
git commit --signoff
git commit -s
```

This will use your default git configuration which is found in `.git/config`. To change
this, you can use the following commands:

```bash
git config --global user.name "Awesome Developer"
git config --global user.email "awesome.developer@example.org"
```

If you have authored a commit that is missing the signed-off-by line, you can amend your
commits and push them to GitHub.

```bash
git commit --amend --signoff
```

If you've pushed your changes to GitHub already you'll need to force push your branch
after this with ``git push -f``.

# Submitting a Pull Request

You can commit your code and push to your forked repo. Once all of your local changes have been tested and formatted, you are ready to submit a PR. For FormS, we use the "Squash and Merge" strategy to merge in PR, which means that even if you make a lot of small commits in your PR, they will all get squashed into a single commit associated with the PR. Please make sure that comments and unnecessary file changes are not committed as part of the PR by looking at the "File Changes" diff view on the pull request page.
    
Once the pull request is submitted, the maintainer will get notified and review your pull request. They may ask for additional changes or comment on the PR. You can always make updates to your pull request after submitting it.
