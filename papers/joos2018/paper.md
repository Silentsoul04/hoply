---
title: 'Versioned Generic Tuple Store'
tags:
  - Database
  - Knowledge Base
  - Python
  - Reproducible Science
  - Version Control System
authors:
  - name: Amirouche Boubekki
    orcid: 0000-0000-0000-0000
    affiliation: 1
affiliations:
 - name: https://hyper.dev
   index: 1

date: 13 Juin 2019
bibliography: paper.bib
---

# Summary

Versioning in production systems is a trick everybody knows about
whether it is through backup, logging systems and ad-hoc audit
trails. It allows to inspect, debug and in worst cases rollback to
previous states. There is no need to explain the great importance of
versioning in software management as tools like mercurial, git and
fossil have shaped modern computing. Having the power of multiple
branch versioning open the door to manyfold applications. It allows to
implement a mechanic similar to github's pull requests and gitlab's
merge requests in many domains. That very mechanic is explicit about
the actual human workflow in entreprise settings in particular when a
senior person validates a change by a less senior person.

Versioning tuples in a direct-acyclic-graph make the implementation of
such mechanics more systematic and less error prone as the
implementation can be shared across various tools and organisations.

Being generic allows downstream applications to fine tune their
time-space requirements. By incrementing the number of items in a
tuple, it allows to easily represent provenance or licence. Thus, it
avoid the need for reification technics as described in `Frey:2017` to
represent metadata on all tuples.

[hoply](https://github.com/amirouche/hoply/) is a prototype that takes
the path of versioning data in a direct-acyclic-graph and apply the
pull request mechanic to collaboration around the making of a
knowledge base, similar in spirit to wikidata.

Resource Description Framework (RDF) offers a good canvas for
cooperation around open data but there is no solution that is good
enough according to `@Canova:2015`.  The use of a version control
system to store open data is a good thing as it draws a clear path for
reproducible science. But none, meets all the expectations. In
projects like [datahub.io](https://datahub.io), hoply aims to replace
the use of git. hoply can make practical cooperation around the
creation, publication, storage, re-use and maintenance of knowledge
bases that are possibly bigger than memory.

hoply use a novel approach to store tuples that is similar in
principle to OSTRICH `@Ruben:2018` in a key-value store. hoply use
[WiredTiger database storage](http://www.wiredtiger.com/) engine to
deliver a pragmatic versatile ACID-compliant versioned generic tuple
store. hoply only stores changes between versions. To resolve
conflicts, merge commits must copy some changes. hoply does not rely
on the theory of patches introduced by Darcs `Tallinn:2005`.

# Current status, and plans for the future

The current implementation explicits that the approach works.  Minimal
benchmarks show that is scales in terms of data size.

[Another implementation in Scheme programming
language](https://github.com/awesome-data-distribution/datae) was
started that has promising results both in terms of speed and space
requirements.

# References
