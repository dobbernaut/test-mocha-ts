# Test with Mocha and Typescript

- [Test with Mocha and Typescript](#test-with-mocha-and-typescript)
  - [Installation](#installation)
    - [Adding node packages](#adding-node-packages)
  - [Prerequisites](#prerequisites)
    - [nvm](#nvm)
      - [For Linux](#for-linux)
      - [For Mac](#for-mac)
      - [For Windows](#for-windows)
  - [Setup](#setup)
  - [Test](#test)
    - [Run specific tests by file or directory](#run-specific-tests-by-file-or-directory)
    - [Run specific tests using grep](#run-specific-tests-using-grep)
    - [Run tests in parallel](#run-tests-in-parallel)
    - [Watch a test file](#watch-a-test-file)
    - [Test tagging](#test-tagging)
    - [Test report](#test-report)
  - [Lint and Format](#lint-and-format)
  - [Structure](#structure)
    - [Artifacts](#artifacts)
    - [Config](#config)
    - [Constants](#constants)
    - [Scripts](#scripts)
    - [Services](#services)
    - [Tests](#tests)

This project runs on [node](https://nodejs.org/en/) and [yarn](https://yarnpkg.com/getting-started) for package management and written on [Typescript](https://www.typescriptlang.org/docs/handbook/typescript-from-scratch.html).

It uses [Mocha](https://mochajs.org/) for the test framework and [Chai](https://www.chaijs.com/) to perform test assertions and [axios](https://axios-http.com/) as an http client for making http or service requests to test.

See [structure](#structure) for a description of the test project structure.

## Installation

Have [nvm](https://github.com/nvm-sh/nvm) installed to make it easier to manage node from your local environment. Yarn is also required to be installed with node and npm. Later versions of node should include yarn by default, if not, follow the install steps from the [prerequisites](#yarn).

```bash
nvm use
yarn install
```

### Adding node packages

Using [yarn install](https://classic.yarnpkg.com/en/docs/cli/install) is used to install all dependencies from this package.

To [add](https://classic.yarnpkg.com/en/docs/cli/add), use `yarn add -D { package-name }` for adding new packages and;

`yarn upgrade { package-name }@{ version-number }` for [upgrading](https://classic.yarnpkg.com/lang/en/docs/cli/upgrade/) existing packages.

## Prerequisites

### [nvm](https://github.com/nvm-sh/nvm)

#### For Linux

```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash

# check nvm installed
command -v nvm

# install node
nvm install node

# or with a specific version
nvm install node 16.13.0

# it default to the first node installed but to use a different version
nvm use 16.13.0

# install yarn globally
npm install -g yarn

# check yarn working properly
yarn --version
```

#### For Mac

```
brew install nvm
nvm install `cat .nvmrc`
nvm use `cat .nvmrc`
```

#### For Windows

Download the setup.zip file from the [latest release](https://github.com/coreybutler/nvm-windows/releases), extract and run setup as administrator.

**Run terminal as administrator to run and use nvm.**

```bash
# check nvm installed
nvm

# install a version of node
nvm install 16.13.0

# use a version of node
nvm use 16.13.0

# check if yarn is installed
yarn -v

# if yarn not installed, install it globally
npm install -g yarn
```

## Setup

To set up the project, install the npm packages by running

```bash
yarn ci
```

## Test

To run all tests:

```bash
yarn test
```

### Run specific tests by file or directory

You can run a specific test by passing a test file or folder to the test-file script. eg:

If you have a test structure like:

```
|-- test
|   |-- scenario1
|       |-- file-1a.test.ts
|       |-- file-1b.test.ts
|   |-- scenario2
|       |-- file-2a.test.ts
```

```bash
# running test-file script passing a directory
yarn test-file test/scenario1
# will run file-1a and file-1b tests

# running test-file script passing a file
yarn test-file test/scenario2/file-2a.test.ts
# will run file-2a test
```

### Run specific tests using grep

You can run a specific test by using the grep option of the test framework. You can specify any string from the describe, context, it blocks of the tests in the format of `yarn test-grep $ANY_STRING_FROM_TEST_BLOCKS` eg:

```bash
# if you have a describe block with a string 'Sample test'
yarn test-grep Sample
```

### Run tests in parallel

You can run all the tests in parallel by using the -parallel script. Other scripts have a -parallel variant if you need to run specific test files or directory with test-file by adding -parallel `yarn test-file-parallel` or with grep `yarn test-file-grep`.

```bash
# adding -parallel to test-file will run all tests under test/scenario1 in parallel
yarn test-file-parallel test/scenario1

```

### Watch a test file

You can have a test or sets of tests to automatically rerun after saving your changes by providing a directory, file or list of both to the watch option.

```bash
yarn test-watch test/scenario1/file-1a.test.ts
```

### Test tagging

We can use the concept of tagging our tests so we can run a select group of tests across the whole test suite. A scenario could be for running smoke tests across all our services.

If we have a smoke test file for each service under test and there are numbers of them; we can tag the describe block of that smoke test file with @smoke and use the test-grep script to run tests that matches this keywork @smoke. eg:

```bash
# this will only run tests where @smoke is in the describe or test string
yarn test-grep @smoke
```

### Test report

There is an html report generated after every run of our tests. These are saved under the `artifacts/report` folder. This should provide additional resource for reviewing the test results outside of the test runner from the console.

## Lint and Format

Formatting and linting of source files are enforced by [eslint](https://eslint.org/docs/about/) and [prettier](https://prettier.io/).

Most editors can integrate directly with these tools, so that files will be checked and formatted.

> On install of local dependencies `yarn install`, a git pre-commit hook will be added from [githooks](scripts/githooks/pre-commit).
> This will run steps similar to `yarn lint-pretty` for the files to be committed.

> **WARNING**: You can add **--no-verify** on your git commit to bypass the pre-commit hook... If you don't use it, that will be the end of it. I will not look for you, I will not pursue you... but if you do, I will look for you, I will find you... and I will kill you.

Before then, the IDE will highlight issues and errors based on rules that were set in [.eslintrc.json](.eslintrc.json) to be fixed, see eslint [rules](https://eslint.org/docs/rules/). Here are the npm scripts to lint and check formatting:

- `yarn lint-pretty` - see if there are linting issues and what files are not formatted correctly.
- `yarn lint-fix-pretty` - try to fix fixable eslint errors and re-format files in place according to the prettier rules.

## Structure

```
.
|-- test
|   |-- artifacts
|       |-- reports
|           |--report_20420908_121213.html
|   |-- config
|       |-- config.ts
|   |-- constants
|   |-- scripts
|   |-- services
|       |-- sample-api-service
|           |-- api-endpoints.ts
|           |-- api-endpoints-helper.ts
|           |-- index.ts
|       |-- aws-utility-service
|           |-- aws.ts
|   |-- tests
|       |-- scenario-group
|           |-- group-1
|               |-- group-1.test.ts
|               |-- group-1a.test.ts
|           |-- group-2
|               |-- group-2.test.ts
|               |-- group-2a.test.ts
|-- package.json
|-- *config.json
```

### Artifacts

- Have all test artifacts save here ie test result reports, error screenshots and logs.

### Config

- All test related configurations should live here. Do not confuse with configs for node packages and dependencies like eslint, mocha configs on the main directory.

### Constants

- Contains constants to use for test and function arguments.
- **Example** http response status codes.

### Scripts

- Any scripts we need to run adjacent to our test suite like hooks or running build pipelines.
- Exceptions could be when a build tool requires their scripts on a specific directory eg: github actions requires them to be on a .github directory from the main directory.

### Services

- All services under test and utilities for the test suite are here. If you need to get data from a web service or a configuration or secret from a key store, create a service folder for that resource or purpose here.
- **Example** if you want to read a json file from aws s3, create an aws utility folder here and have an s3.ts file that contains methods for getting files from s3 etc.

```
|-- services
|   |-- aws
|       |-- s3.ts
|   |-- kafka
|       |-- kafka.ts
|   |-- blog-post
|       |-- blog-post.ts
|       |-- blog-post-helper.ts
|       |-- index.ts
```

- **index.ts** - Use index to export all files from a directory so there's not much clutter from the import statements when importing a few classes or methods from files spread inside the directory eg:

```
|-- service
|   |-- blog-post
|       |-- blog-post.ts
|       |-- blog-post-helper.ts
|       |-- index.ts
```

```javascript
// blog-post.ts

export const blogPostFunction = () => {
  console.log('hello from blog post function');
};
```

```javascript
// blog-post-helper.ts

export const blogPostFunctionHelper = () => {
  console.log('hello from blog post function helper');
};
```

```javascript
// index.ts

export * from './blog-post';
export * from './blog-post-helper';
```

```javascript
// file.test.ts

import { blogPostFunction, blogPostFunctionHelper } from './services/blog-post';
```

### Tests

- All tests for your application are here. Suffix the test files with .test.ts. Try grouping them by logical parts of the application or service.

```
|-- tests
|   |-- blog-posts
|       |-- add-update.test.ts
|       |-- delete.test.ts
|       |-- get.test.ts
```
