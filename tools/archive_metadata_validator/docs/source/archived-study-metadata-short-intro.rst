Metadata for Archived Studies - Short Intro
===========================================

This is a short introduction to the machine readable notation that is used
for archived study metadata. It will help you to enter information in the
provided issue template (please read the `Archived Study Metadata Handbook
<Metadata-for-Archived-Studies-Handbook>`_,
ca. 10 minutes reading time, for a more detailed explanation of the archived
study metadata elements).

Generally information is entered by specifying an element name followed by a
colon and a space and then the content. For example::

    name: An Important Study

defines the element "name" with the content "An Important Study" (as demonstrated
in the "purpose"-element below, text can also span multiple lines).

All example text is surrounded with <ex> and </ex>. Please replace the example
text including the <ex> and </ex> with your data. For example, replace::

     name: <ex>Name of study</ex>

with::

     name: Intelligence in Rodents

if the name of your study is "Intelligence in Rodents".

An element might contain another element. This is indicated by indenting the
contained element. For example::

   study:
       name: A name
       start_date: 1.1.2020

defines an element named study that contains itself two elements, i.e.
the element "name" with the content "A name", and the element "start_date"
with the content "1.1.2020".

In addition to named elements there are lists. Lists have a name and their
contained elements are indented and preceded by a "-". For example::

    keywords:
       - Rodents
       - Navigation

defines a list named "keywords" with two entries, i.e. "Rodents", and
"Navigation". Lists in this template contain only one or two entries for
demonstration purposes. You can extend lists with additional elements or delete
elements from the list, but you can not create empty lists. If you do not want,
in this example, to specify keywords, just delete the line with "keywords:" and
all list entries, i.e. the lines with "Rodents" and with "Navigation".

Generally if you want to delete an element you have to delete the contained elements
as well, that means everything that is indented. For example, if you delete "b" from
the following::

    a: example text
    b:
       b1: more example text
       b2: even more example text
    c:
       c1: text example

you end up with::

    a: example text
    c:
       c1: text example

and if you want to delete "a" from the above document, you end up with::

    c:
       c1: text example

You do not have to enter information for all elements. Most elements are optional
and can be deleted.  Comments indicate which element is optional and can be deleted.

**One last note**: content that contains a colon followed by space, e. g. Tel: +49 1 555333
has to be enclosed in double quotes like this: "Tel: +49 1 555333".

