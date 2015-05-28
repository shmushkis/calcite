# Apache Calcite docs site

This directory contains the code for the Apache Calcite (incubating) web site,
[calcite.incubator.apache.org](https://calcite.incubator.apache.org/).

## Setup

1. `cd site`
2. svn co https://svn.apache.org/repos/asf/incubator/calcite/site target
3. `sudo apt-get install rubygems ruby2.1-dev zlib1g-dev` (linux)
4. `sudo gem install bundler`
5. `sudo gem install github-pages jekyll`
6. `bundle install`

## Add javadoc

1. `cd ..`
2. `mvn -DskipTests site`
3. `mv target/site/apidocs site/target`

## Running locally

Before opening a pull request, you can preview your contributions by
running from within the directory:

1. `bundle exec jekyll serve`
2. Open [http://localhost:4000](http://localhost:4000)

## Pushing to site

1. `cd site/target`
2. `git status`
3. You'll need to `git add` any new files
4. `git commit -a`
5. `git push origin asf-site`
