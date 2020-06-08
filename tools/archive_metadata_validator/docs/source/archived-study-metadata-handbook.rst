..
    Long lines ahead!
    In order to keep commits to this file comprehensible paragraphs
    are written in a single line, i.e. no hard word wrap.

    If you work with a limited number of columns, please enable
    soft-wrap on your editor.


**************************************
Metadata for Archived Studies Handbook
**************************************

Version 1.3

This document describes the metadata for archived studies. More precisely, it describes the metadata elements that can be used, and the machine-readable format in which they are specified. The schema is deliberately kept simple in order to facilitate quick metadata capturing, even from older studies. At the same time, it allows for more detailed information, if available.

Metadata Elements
=================

The metadata for archived studies (MAS) contains only a few elements, most of which are optional. The top-level element is a Study. This element in turn contains a number of entries, which might themselves be elements.

There are six high-level elements: study, dataset, person, contributor, publication, and person reference. This section gives an overview of the elements in MAS. For each sub-element we give the name, whether it is optional, the type of data that it should contain, e.g. text or integer, and optionally an explanation of what it should describe. So for example:

- Purpose (optional): text, purpose of study

describes an element named Purpose, which is optional, its type is "text", and it should describe the "purpose of the study".


Study
-----

These are the elements that comprise the study-element:

- Name: text, name of the study
- Dataset: Dataset-element (cf. below)
- Purpose: (optional): text, purpose of the study, could facilitate location of the study via search engines
- Start date (optional): text, date on which the study was started, e.g. "23.1.2019"
- End date (optional): text, date on which the study was ended, e.g. "25.3.2020"
- Keywords (optional): list of texts
- Persons (optional): list of Person-elements, persons that were involved in the study
- Publications (optional): list of Publication-elements
- Contributors (optional): list of Contributor-elements, which define persons and their role in the execution of the study


Dataset
-------

These are the elements that comprise the dataset-element:

- Name of the dataset: text
- URL of the dataset: text, where is the unarchived dataset stored
- Description (optional): text, could facilitate location of the dataset via search engines
- Data format (optional): text
- Keywords (optional): list of texts
- Contributors (optional): list of Contributor-elements (cf. below), defines persons and their role in the creation of the data

Person
------

These are the elements that comprise the person-element:

- First name: text
- Last name: text
- Email: text, institutional email. Should globally uniquely identify the person.
- Title (optional): text
- Affiliation (optional): text
- ORCID ID (optional): text
- Additional contact information (optional): text

Contributor
-----------
A Contributor-element combines a person reference with a role that this person held, e.g. the person might be the data curator for the dataset of a study. These are the elements that comprise the Contributor-element:

- Person: person reference (cf. below), the person that contributed
- Roles: list of text, the roles in which the person contributed.

Roles should be taken from the following list of predefined roles, if applicable at all: *Author*, *CorrespondingAuthor*, *DataCurator*, *DataCollector*, *Maintainer*, *ProjectLeader*, *ProjectMember*, *Researcher*, *SoftwareDeveloper*.

Nevertheless, if none of the predefined roles describes the role of the contributing person correctly, you can provide your own description.


Publication
-----------
These are the elements that comprise the publication-element:

- Corresponding Author: person reference (cf. below)
- Title: text, the title of the publication
- Date: text, date of the publication
- Authors (optional): list of person references, additional authors
- DOI (optional): text, if a DOI is present, it will be used as an authoritative source for the remaining properties in this element and for entries in the persons-list.
- Publication (optional): text, the name of the publication
- Volume (optional): text, the volume number of the publication
- Issue (optional): text, the issue number of the publication
- Pages (optional): text, pages on which the publication appeared
- Publisher (optional): text, entity that published the publication

Person Reference
--------------------
Person references are used in the Study-element, the Dataset-element and in the Publication-element. They reference persons that are contained in the persons-list of the Study-element. That means, person information is kept at a single place in MAS, i.e. in the persons list of the Study-element. Other places in MAS that require person information just refer to entries in this list.

Entries in the persons list are referred to either by the person’s email or by the person’s first and last name.

Since there are two ways to reference a person (either by their email, or by their first and last name), a person reference can be either of the following elements:

- Email: text

or

- First name: text

  Last name: text


Please check the section `Example 1: Complete Metadata Description`_ for an example.

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
    url: http://www.example.com/


The given code defines two entities, namely "name" and "url", with the respective content "This is a name" and "http://www.example.com/".

Long context can also be written into multiple lines, for example, the following code snippet defines an element named "description" with the content "Lorem ipsum ... ullamco"::

    description:
      Lorem ipsum dolor sit amet, consectetur adipisici elit,
      sed eiusmod tempor incidunt ut labore et dolore magna aliqua.
      Ut enim ad minim veniam, quis nostrud exercitation ullamco

**Please note**: if the content of an element contains the character colon followed by space, you have to enclose the content into double-quotes. For example when you specify the element additional_contact_info like this::

    addition_contact_info: Tel: +1 555 201-4444

you would see an error, because "Tel" would be mistaken for an element name, since it is followed by a colon and a space.

To prevent this error, you have to enclose the string `Tel: +1 555 201-4444` into double-quotes like this::

    addition_contact_info: "Tel: +1 555 201-4444"


Example for concept 2:
----------------------
An example for the second concept, i.e. indentation of contained elements, is given here::

    study:
      name: Navigational maps in rat brains
      dataset:
        name: fMRI rat navigation
        url: http://www.example.com/studies/images/rat-navigation
      purpose: determine how rats learn to navigate in new environments

The code-snippet above defines an element called "study" that contains three sub-elements, i.e. "name", "dataset", and "purpose". Of which "dataset" itself contains two sub-elements, i.e. "name", and "url".

**Please note**: only use spaces for indentation, not tabulators!

Example for concept 3:
----------------------
An example for the third concept, i.e. lists, is given here::

    keywords:
      - fMRI
      - Rodents

The code snippet above defines an element named "keywords", that contains a list with two elements, i.e. the two text strings "fMRI" and "Rodents". Each list element is introduced with a "-", i.e. a minus-sign.

The content of list elements is not restricted to simple types like text strings or number. List elements themselves can be elements with sub-elements, as shown here::

    study:
      persons:
        - person:
            first_name: John
            last_name: Harris
        - person:
            first_name: Ida
            last_name: Miller

The code snippet above defines an element named "study", that contains an element named "persons", that contains a list with two entries. In this example each list entry is itself an element containing other elements. More specific each list entry is a "person" element, that contains two sub-elements, i.e. "first_name" and "last_name" with the respective first and last names.

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
        purpose:                                 # purpose can be deleted
            Identify what determines intelligence
            in rodents and whether it is related
            to food.
        start_date: 1.1.1990                     # Can be deleted
        end_date: 1.1.2010                       # Can be deleted

        keywords:                                # keywords can be deleted
            - Rodent
            - Intelligence
            - Food                               # All but the first keyword entry can be deleted

        persons:                                 # persons can be deleted
            - person:
                first_name: Hans
                last_name: Glück
                email: hg@fz-juelich.de
                title: Prof. Dr.                                   # Can be deleted
                affiliation: FZ-Jülich                             # Can be deleted
                orcid-id: 1000-0002-4092-0601                      # Can be deleted
                additional_contact_information: "Tel: +49 111 5553433"       # Can be deleted
            - person:
                first_name: Irmgard
                last_name: Glöckner
                email: ig@fz-juelich.de
                title: Dr. Dr.                                     # Can be deleted
                affiliation: FZ-Jülich                             # Can be deleted
                orcid-id: 2000-0002-4092-0249                      # Can be deleted

        contributors:                                              # Can be deleted
            - person: hg@fz-juelich.de
              roles:
                  - StudyLeader
                  - CorrespondingAuthor

        publications:                                              # publications can be deleted
            - publication:
                corresponding_author: hg@fz-juelich.de
                title: Food-based intelligence induction in rodents
                date: 1.1.1995
                authors:                                           # authors can be deleted
                    - first_name: Irmgard
                      last_name: Glöckner
                    - hg@fz-juelich.de
                doi: doi:example/p1                                # Can be deleted
                publication: Proceedings in rodent behavior        # Can be deleted
                volume: 23                                         # Can be deleted
                issue: 4                                           # Can be deleted
                pages: 11-15                                       # Can be deleted
                publisher: Spraddison                              # Can be deleted

            - publication:                                         # All but the first publication entry can be deleted
                corresponding_author: hg@fz-juelich.de
                title: Rodent studies survey
                date: 1.1.1998
                doi: doi:example/p2
                publication: Intelligence Research
                volume: 33
                issue: 9
                publisher: Elsberg
                pages: 233-244

        dataset:
            name: Rodent-Intelligence Brainscans
            url: file:/bulk1:/data/ristudy
            description:                               # description can be deleted
                Lorem ipsum dolor sit amet,
                incidunt ut labore et dolore
                nostrud exercitation ullamco
            data_format: DICOM                         # Can be deleted

            keywords:                                  # keywords can be deleted
                - fMRI
                - Rodents                              # All but the first keyword entry can be deleted

            contributors:                              # contributors can be deleted
                - person: ig@fz-juelich.de
                  roles:
                      - CorrespondingAuthor
                      - DataCurator                    # All but the first role can be deleted

The example above illustrates the purpose of the persons-element. It lists all persons that have a role in the creation of the study, in the creation of the dataset, or in the creation of a publication. Detailed person information is listed in the persons-element of the study.

This information is referred to within the author and corresponding_author-element of the publication-element. Persons are also referenced within the study-element and the dataset-element, where they are associated with a list of roles, that the person holds in the respective content. References allow you to use a person in several contexts without repeating the definition of him over and over.

Within MAS persons are referred to by either their email, or their first and last name. For example in the authors list of the publication with the title "Food-based intelligence induction in rodents", we refer to the person with the first name "Irmgard" and the last name "Glöckner" as an author like this::

    ...
    authors:
        - first_name: Irmgard
          last_name: Glöckner
        - ...
    ...

In the corresponding_author-element of the publication with the title "Rodent studies survey" we instead use the email of Irgmard Glöckner::

    ...
    corresponding_author: ig@fz-juelich.de

You are free to use either form of reference. NB: you may or may not repeat the corresponding_author in the optional author-element, if a person apears as one of the authors and as corresponding author, this person is assumed to be the corresponding author.

Example 2: Minimal Metadata Description
---------------------------------------

The following show the minimal possible metadata description, i.e. the metadata description in which all optional elements are left out::

    study:
        name: Intelligence in Rodents
        dataset:
            name: Rodent-Intelligence Brainscans
            url: http://www.example.com/data/ristudy

Questions?
==========
If you have any questions, please contact: c.moench@fz-juelich.de.
