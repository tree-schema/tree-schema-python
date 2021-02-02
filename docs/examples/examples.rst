Example Usage
=============

The most powerful feature in the Tree Schema API is it's ability to 
programatically create and manage data lineage. Most of the other 
features: managing data stores, schemas and fields are much more 
efficient to do through the Tree Schema GUI because you can point 
Tree Schema to your database and Tree Schema will collect the 
metadata on your behalf and populate your data catalog for you. 
You can learn about the automated connections in the 
first two sections of the 
`Tree Schema 101 <https://treeschema.com/blog/tree-schema-101/>`_
tutorial.

However, in order to build data lineage programatically you 
must first have an understanding of the data stores, schemas and 
fields that are linked together. These examples provide a 
comprehensive overview of all of the API features.

.. toctree::
   :maxdepth: 1

   data_stores
   data_schemas
   fields
   sample_values
   transformations
   transformation_links
   advanced_transformations
   data_lineage
   dbt
   