<!---
Long lines ahead!
In order to keep commits to this file comprehensible paragraphs
are written in a single line, i.e. no hard word wrap.

If you work with a limited number of columns, please enable
soft-wrap on your editor.
-->

#Metadata for Archived Studies Handbook

This document describes the metadata for archived studies. More precisely, it describes the metadata elements that can be used, and the machine-readable format in which they are specified. The schema is deliberately kept simple in order to facilitate quick metadata capturing, even from older studies. At the same time, it allows for more detailed information, if available.

## Metadata Elements

The metadata for archived studies (MAS) contains only a few elements, most of which are optional. The top-level element is a Study. This element in turn contains a number of entries, which might themselves be elements.

There are only five high-level elements: study, dataset, person, publication, and person reference. This section gives an overview over the elements in MAS. For each sub-element we give the name, whether it is optional, the type of data that it should contain, e.g. text or integer, and optionally an explanation what it should describe. So for example:

- Purpose (optional): text, purpose of study

describes an element named Purpose, which is optional, its type is "text", and it should describe the "purpose of the study".


### Study
These are the elements that comprise the study-element:

- Name: text, name of the study
- Dataset: Dataset-element (cf. below)
- Purpose: (optional): text, purpose of the study
- Start date (optional): text, date on which the study was started, e.g. "23.1.2019"
- End date (optional): text, date on which the study was ended, e.g. "25.3.2020"
- Keywords (optional): list of texts
- Persons (optional): list of Person-elements, persons that were involved in the study
- Publications (optional): list of Publication-elements
- Contact person (optional): person reference (cf. below), reference to an entry in the list of persons that were involved in the study

### Dataset
These are the elements that comprise the dataset-element:

- Name of the dataset: text
- URL of the dataset: text
- Description (optional): text
- Data format (optional): text
- Availability (optional): text, one of: "public", "private", "controlled"
- License (optional): text, describe the license under which the data can be used
- Keywords (optional): list of texts
- Contact person (optional): person reference (cf. below)

The availability-element, if present, should reflect whether the dataset is publicly available ("public"), not available ("private"), or only available after requesting access ("controlled"). In the latter case, contact person information should be provided and the contact person should be contacted for access.

### Person
These are the elements that comprise the person-element:

- First name: text
- Last name: text
- ID (optional): text, an internal ID that can be used to refer to a person within MAS (cf. Person-reference)
- Title (optional): text
- Affiliation (optional): text
- Role (optional): text, the role of the person with the context of the study
- Email (optional): text
- ORCID ID (optional): text
- Additional contact information (optional): text

### Publication
These are the elements that comprise the publication-element:

- Authors: list of person references (cf. below)
- Title: text, the title of the publication
- Date: text, date of the publication
- DOI (optional): text, the digital object identifier of the publication
- Publication (optional): text, the name of the publication
- Volume (optional): integer, the volume number of the publication
- Issue (optional): integer, the issue number of the publication
- Pages (optional): text, pages on which the publication appeared
- Publisher (optional): text, entity that published the publication

### Person Reference
Person references are used in the Study-element, the Dataset-element and in the Publication-element. They reference persons that are contained in the persons-list of the Study-element. That means, person information is kept at a single place in MAS, i.e. in the persons list of the Study-element. Other places in MAS that require person information just refer to entries in this list.

Entries in the persons list are referred to either by first and last name, or by an identifier. That is what the id property in a person-element is used for. You can assign a unique (within the scope of the metadata file) value, for example: "person-1",  and use this to refer to this person later. The ID-element can also be use to disambiguate persons that have the same first and last name.

Since there are two ways to reference a person (either by first and last name, or by their ID), a person reference can be either of the following elements:

- First name: text
- Last name: text

or

- Id: text

Please check the section "Example 1: Complete Metadata Description" for an example.

## Metadata format
This section describes, how the elements described in the previous section can be specified in a digital document. We use a text-based format, i.e. YAML, that is rather intuitive. The three main concepts to keep in mind are:
 
 1. Elements are identified by a name followed by a colon and a space and the element content
 
 2. Elements that are contained within other elements are indented, for example by two spaces (identical indentation levels mean identical containing element)
 
 3. Lists are marked by a list of "-" characters, that precedes each list entry.

####Example for concept 1:
An example for the first concept, i.e. names and content, is given here:

<pre>
name: This is a name
url: http://www.example.com/
</pre>

The given code defines two entities, namely "name" and "url", with the respective content "This is a name" and "http://www.example.com/".

Long context can also be written into multiple lines, for example, the following code snippet defines an element named "description" with the content "Lorem ipsum ... ullamco":

<pre>
description:
  Lorem ipsum dolor sit amet, consectetur adipisici elit,
  sed eiusmod tempor incidunt ut labore et dolore magna aliqua.
  Ut enim ad minim veniam, quis nostrud exercitation ullamco
</pre>

**Please note**: if the content of an element contains the character colon followed by space, you have to enclose the content into double-quotes. For example when you specify the element additional_contact_info like this:

<pre>
addition_contact_info: Tel: +1 555 201-4444
</pre>

you would see an error, because "Tel" would be mistaken for an element name, since it is followed by a colon and a space.

To prevent this error, you have to enclose the string `Tel: +1 555 201-4444` into double-quotes like this:

<pre>
addition_contact_info: "Tel: +1 555 201-4444"
</pre>

####Example for concept 2:
An example for the second concept, i.e. indentation of contained elements, is given here:

<pre>
study:
  name: Navigational maps in rat brains
  dataset:
    name: fMRI rat navigation
    url: http://www.example.com/studies/images/rat-navigation
  purpose: determine how rats learn to navigate in new environments
</pre>

The code-snippet above defines an element called "study" that contains three sub-elements, i.e. "name", "dataset", and "purpose". Of which "dataset" itself contains two sub-elements, i.e. "name", and "url".

**Please note**: only use spaces for indentation, not tabulators!

####Example for concept 3:
An example for the third concept, i.e. lists, is given here:

<pre>
keywords:
  - fMRI
  - Rodents
</pre>

The code snippet above defines an element named "keywords", that contains two text strings, i.e. "fMRI" and "Rodents". Each list element is introduced with a "-", i.e. a minus-sign. 

The content of list elements is not restricted to simple types like text strings or number. List elements themselves can be elements with sub-elements, as shown here:
<pre>
study:
  persons:
    - person:
        first_name: John
        last_name: Harris
    - person:
        first_name: Ida
        last_name: Miller
</pre>
The code snippet above defines an element named "study", that contains an element named "persons", that contains a list with two entries. In this example each list entry is itself an element containing other elements. More specific each list entry is a "person" element, that contains two sub-elements, i.e. "first_name" and "last_name" with the respective first and last names.

## Complete Metadata Definition
Instead of formally describing the MAS format using some kind of schema language, we use the informal description given in the section "Metadata Elements" above and two examples given below. The first example shows a complete metadata description, i.e. a description that contains all elements that MAS defines. The second example is the minimal required set of elements in MAS.

If you keep in mind:

1. All elements marked as optional in section "Metadata Elements" can be left out, no matter whether they contain sub-elements or not.

2. All lists can have arbitrary many entries (at least one), so an arbitrary number of elements can be added, e.g. the persons element could contain a list of 2000 persons.

you should be able to quickly generate a metadata description of your study by modifying the complete example, i.e. modifying element content or deleting elements that you do not need and that are marked as optional above. Please note, the example use four spaces for the next indentation level. Feel free to change this number as long as it is consistent, i.e. sub-elements of an element all have the same indentation.

#### Example 1: Complete Metadata Description
<pre>
study:
    name: Intelligence in Rodents
    purpose:
        Identify what determines intelligence in rodents and
        Whether it is related to food.
    start_date: 1.1.1990
    end_date: 1.1.2010
    keywords:
        - Rodent
        - Intelligence
        - Food
    contact_person:
        first_name: Hans
        last_name: Glück
    persons:
        - person:
            id: person-1
            first_name: Hans
            last_name: Glück
            title: Prof. Dr.
            affiliation: FZ-Jülich
            role: Study Leader
            email: hg@fz-juelich.de
            orcid-id: 1000-0002-4092-0601
            additional_contact_information: "Tel: +49 111 5553433"
        - person:
            id: person-2
            first_name: Irmgard
            last_name: Glöckner
            title: Dr. Dr.
            affiliation: FZ-Jülich
            role: Scientist
            orcid-id: 2000-0002-4092-0249
            email: ig@fz-juelich.de
    publications:
        - publication:
            doi: doi:example/p1
            authors:
                - person-1
                - first_name: Irmgard
                  last_name: Glöckner
            title: Food-based intelligence induction in rodents
            publication: Proceedings in rodent behavior
            volume: 23
            issue: 4
            pages: 11-15
            publisher: Spraddison
            date: 1.1.1995
        - publication:
            doi: doi:example/p2
            authors:
                - person-1
                - person-2
            title: Rodent studies survey
            publication: Intelligence Research
            volume: 33
            issue: 9
            publisher: Elsberg
            pages: 233-244
            date: 1.1.1998
    dataset:
        name: Rodent-Intelligence Brainscans
        url: file:/bulk1:/data/ristudy
        description:
            Lorem ipsum dolor sit amet, consectetur adipisici elit, sed eiusmod tempor
            incidunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
            nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi
            Consequat
        data_format: DICOM
        availability: public
        license: Creative Commons 1.0
        keywords:
            - fMRI
            - Rodents
        contact_person: person-2
</pre>

The example above illustrates the purpose of the persons-element. It lists all persons that are either involved in the project, and/or are among the authors of a publication, and/or are contact points for inquiries about the study or dataset. In the latter two cases, i.e. authors and contact points, the respective person is just referenced. This allows to use a person in several contexts without repeating the defintion of him over and over.

Person references is also where the id-element in the person-element becomes relevant, and the example illustrates its usage. For example the person with the first name "Irmgard" and the last name "Glöckner" has been assigned the id "person-2". We refer to Irmgard Glöckner in authors list of the publication with the title "Food-based intelligence induction in rodents" by her first and last name, i.e.:

<pre>
    ...
    authors:
        - first_name: Irmgard
          last_name: Glöckner
        - ...
    ...
</pre>

In the author list of the publication with the title "Rodent studies survey" we instead use the id, i.e. "person-2", that we assigned to Irgmard Glöckner:

<pre>
    ...
    authors:
        - person-2
        - ...
    ...
</pre>

You are free to use either way of reference.

#### Example 2: Minimal Metadata Description
The following show the minimal possible metadata description, i.e. the metadata description in which all optional elements are left out.

<pre>
study:
    name: Intelligence in Rodents
    dataset:
        name: Rodent-Intelligence Brainscans
        url: http://www.example.com/data/ristudy
</pre>

##Questions?

If you have any questions, please contact: <mailto:c.moench@fz-juelich.de>.
