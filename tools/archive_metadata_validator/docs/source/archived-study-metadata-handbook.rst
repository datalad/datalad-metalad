..
    Long lines ahead!
    In order to keep commits to this file comprehensible, paragraphs
    are written in a single line, i.e. there is no hard word wrap.

    If you work with a limited number of columns, please enable
    soft-wrap on your editor.


**************************************
Metadata for Archived Studies Handbook
**************************************

Version 1.4

This document describes the metadata for archived studies. More precisely, it describes the metadata elements that can be used, and the machine-readable format in which they are specified. The schema is deliberately kept simple in order to facilitate quick metadata capturing, even from older studies. At the same time, it allows for more detailed information, if available.

Metadata Elements
=================

The metadata for archived studies (MAS) contains only a few elements, most of which are optional. There are four top-level elements: study, dataset, publication, and persons. These elements contain a number of possible entries, which might themselves be elements. Not all elements are mandatory, quite a number of them are in fact optional and don't have to be specified

This section gives an overview of the elements in MAS. For each element and sub-element we give the name, whether it is optional, the type of data that it should contain, e.g. text or integer, and optionally an explanation of what it should describe. So for example:

- Purpose (optional): text, purpose of study

describes an element named Purpose, which is optional, its type is "text", and it should describe the "purpose of the study".


Study
-----

The Study-element is required in the metadata specification. It consists of the following sub-elements:

- Name: text, name of the study
- Principal investigator: person-reference (cf. below), the name of the principal investigator, i.e. the person responsible for the study
- Keyword: text or list of text, at least one keyword is required
- Purpose: (optional): text, purpose of the study, could facilitate location of the study via search engines
- Start date (optional): text, date on which the study was started, e.g. "23.1.2019"
- End date (optional): text, date on which the study was finished, if it was finished yet"
- Contributor (optional): person-reference or list of person-references, persons that contributed to the study
- Funding (optional): text or list of texts, name(s) of entities that funded the study, e.g "DFG"

Person references are emails. The Person-element described in the chapter `Person`_ contains a set of email-addresses and information about the person that is addressed by the email-address. A person-reference is one of the emails in the Person-element.

Please see the section `Example 1: Complete Metadata Description`_ for an example.


Dataset
-------

The dataset-element is required in the metadata specification. These are the elements that comprise the dataset-element:

- Name: text, the name of the dataset
- Location: text, location where is the unarchived dataset stored
- Author: person-reference or list of person-references, persons that have participated in the collection and creation of the dataset
- Keyword: (optional): text or list of text
- Description (optional): text, could facilitate location of the dataset via search engines
- Standard (optional): list of text, data format standards that are used in the dataset
- Funding (optional): text or list of texts, name(s) of entities that funded the creation of the dataset, e.g "NIH"

Standard should be taken from the following list of identified standards, if applicable at all: *dicom*, *nifti*, *bids*. Nevertheless, if none of the identified standards were used in your dataset, or if you used additional standards, you can provide your own description.

Please see the section `Example 1: Complete Metadata Description`_ for an example.


Person
------

A person element consists of a set of email-addresses and associated information about the owners of the email-addresses. For each email-address, the following information has to be provided:

- Given name: text
- Last name: text
- ORCID ID (optional): text
- Title (optional): text
- Affiliation (optional): text
- Contact information (optional): text, additional contact information, e. g. a telephone number or an address

Please see the section `Example 1: Complete Metadata Description`_ for an example.


Publication
-----------
The publication-element contains a list of records with the following elements:

- Title: text, the title of the publication
- Author: person reference or list of person reference, authors of the publication
- Year: Number, the year of the publication
- DOI (optional): text, if a DOI is present, it will be used as an authoritative source for the remaining properties in this publication and for entries in the persons-list.
- Corresponding author (optional): person-reference, one of the person references in the author-element of this publication
- Publication (optional): text, journal name or publication venue label
- Volume (optional): text, the volume number of the publication
- Issue (optional): text, the issue number of the publication
- Pages (optional): text, pages on which the publication appeared
- Publisher (optional): text, entity that published the publication

Please see the section `Example 1: Complete Metadata Description`_ for an example.


Metadata format
===============
This section describes how the elements described in the previous section can be specified in a digital document. We use a text-based format, i.e. YAML, that is rather intuitive. The three main concepts to keep in mind are:
 
1. Elements are identified by a name followed by a colon and a space and the element content
 
2. Elements that are contained within other elements are indented, for example by two spaces (identical indentation levels mean identical containing element)
 
3. Lists are marked by a list of "-" characters, that precedes each list entry.


Example for concept 1:
----------------------
An example for the first concept, i.e. names and content, is given here::

    name: This is a name
    location: http://www.example.com/


The given code defines two entities, namely "name" and "location", with the respective content "This is a name" and "http://www.example.com/".

Long context can also be written into multiple lines, for example, the following code snippet defines an element named "description" with the content "Lorem ipsum ... ullamco"::

    description:
      Lorem ipsum dolor sit amet, consectetur adipisici elit,
      sed eiusmod tempor incidunt ut labore et dolore magna aliqua.
      Ut enim ad minim veniam, quis nostrud exercitation ullamco

**Please note**: if the content of an element contains the character colon followed by space, you have to enclose the content into double-quotes. For example when you specify the element additional_contact_info like this::

    contact_information: Tel: +1 555 201-4444

you would see an error, because "Tel" would be mistaken for an element name, since it is followed by a colon and a space.

To prevent this error, you have to enclose the string `Tel: +1 555 201-4444` into double-quotes like this::

    contact_information: "Tel: +1 555 201-4444"


Example for concept 2:
----------------------
An example for the second concept, i.e. indentation of contained elements, is given here::

    person:
      a@fz-juelich.de:
        given_name: Hans
        last_name: Glück

      b@fz-juelich.de:
         given_name: Irmgard
         last_name: Glöckner

The code-snippet above defines an element called "person" that contains two sub-elements, i.e. "a@fz-juelich.de", and "b@fz-juelich.de". The "a@fz-juelich.de" sub-element contains two further sub-elements: "given_name", and "last_name" with the respective content "Hans", and "Glück". The "b@fz-juelich.de" sub-element also contains the sub-elements: "given_name", and "last_name" with the respective content "Irmgard", and "Glöckner".

**Please note**: only use spaces for indentation, not tabulators!

Example for concept 3:
----------------------
An example for the third concept, i.e. lists, is given here::

    keywords:
      - fMRI
      - Rodents

The code snippet above defines an element named "keywords", that contains a list with two elements, i.e. the two text strings "fMRI" and "Rodents". Each list element is introduced with a "-", i.e. a minus-sign.

The content of list elements is not restricted to simple types like text strings or number. List elements themselves can be elements with sub-elements, as shown here::

    publication:
      - title: Food-based intelligence
        author: a@fz-juelich.de
        year: 1995

      - title: Rodent studies survey
        author: a@fz-juelich.de
        year: 2005


The code snippet above defines an element named "publication", that contains a list with two entries. Both entries have ths sub-elements "title", "author", and "year".

Complete Metadata Definition
============================
Instead of formally describing the MAS format using some kind of schema language, we use the informal description given in the section "Metadata Elements" above and two examples given below. The first example shows a complete metadata description, i.e. a description that contains all elements that MAS defines. The second example is the minimal required set of elements in MAS.

If you keep in mind:

1. All elements marked as optional in section "Metadata Elements" can be left out, no matter whether they contain sub-elements or not.

2. All lists can have arbitrary many entries (at least one), so an arbitrary number of elements can be added, e.g. the persons element could contain a list of 2000 persons.

you should be able to quickly generate a metadata description of your study by modifying the complete example, i.e. modifying element content or deleting elements that you do not need and that are marked as optional above. Please note, the example uses four spaces for the next indentation level. Feel free to change this number as long as it is consistent, i.e. sub-elements of an element all have the same indentation.

Example 1: Complete Metadata Description
----------------------------------------
::

    study:
      name: Intelligence in Rodents
      purpose:
        Identify what determines intelligence
        in rodents and whether it is related
        to food.
      start_date: 31.10.1990
      end_date: 22.12.2010
      keyword:
        - Rodent
        - Intelligence boost
        - Food
      principal_investigator: a@fz-juelich.de
      contributor:
        - b@fz-juelich.de
        - c@fz-juelich.de
      funding:
        - DFG Rodent Study Funds
        - NIH International

    dataset:
      name: Rodent-Intelligence Brainscans
      location: juseless:/data/project/riskystudy
      description:
        Lorem ipsum dolor sit amet,
        incidunt ut labore et dolore
        nostrud exercitation ullamco
      standard:
        - dicom
        - nifti
        - bids
      keyword:
        - fMRI
        - Rodents
      author:
        - a@fz-juelich.de
        - b@fz-juelich.de
      funding: DFG data funds

    publication:
      - title: Food-based intelligence
        author:
          - a@fz-juelich.de
          - c@fz-juelich.de
        year: 1995
        corresponding_author: a@fz-juelich.de
        doi: doi:example/p1
        publication: Proceedings in rodents
        volume: 23
        issue: 4
        pages: 11-15
        publisher: Spraddison

      - title: Rodent studies survey
        author: a@fz-juelich.de
        year: 2005

    person:
      a@fz-juelich.de:
        given_name: Hans
        last_name: Glück
        orcid-id: 1000-0002-4092-0601
        title: Prof. Dr.
        affiliation: FZ-Jülich
        contact_information:
          Used to work with X, but then went
          to Australia to work with Koalas,
          try calling +1 234 567 890

      b@fz-juelich.de:
         given_name: Irmgard
         last_name: Glöckner
         orcid-id: 2000-0002-4092-0249

      c@fz-juelich.de:
         given_name: Willy
         last_name: Mann

The example above illustrates the purpose of the persons-element. It lists all persons that are referenced as author, contributor, corresponding authoer, or principal investigator. Detailed person information is listed under the email-addresses of the respective person.

Within MAS persons are referred to their email. For example in the authors list of the publication with the title "Food-based intelligence", we refer the corresponding author with his email-address::

    ...
    corresponding_author: a@fz-juelich.de

NB: the corresponding author has to be in the author-list of the respective publication.

Example 2: Minimal Metadata Description
---------------------------------------

The following show the minimal possible metadata description, i.e. the metadata description in which all optional elements are left out::

    study:
      name: Intelligence in Rodents
      start_date: 31.10.1990
      end_date: 22.12.2010
      keyword:
        - Rodent
      principal_investigator: a@fz-juelich.de

    dataset:
      name: Rodent-Intelligence Brainscans
      location: juseless:/data/project/riskystudy
      keyword:
        - fMRI
      author:
        - a@fz-juelich.de

    person:
      a@fz-juelich.de:
        given_name: Hans
        last_name: Glück

Questions?
==========
If you have any questions, please contact: c.moench@fz-juelich.de.
