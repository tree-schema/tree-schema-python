Advanced Transformations & Data Lineage
=======================================

This page shows how to use the Tree Schema library ina `slightly` more advanced,
and practical, manner.

.. note:: All of the data assets in these examples already exist 
   in the Tree Schema organization being used, make sure you are 
   referencing data stores, schemas and fields that exist within 
   your Tree Schema account


Capture Lift and Shift ETL
--------------------------

A common pattern for data movement is extracting all data from one data source 
and dumping it into another. These ETL/ELT jobs typically have very little to 
no change on the field names themeselves. 

The script below is practical solution when the field names do not change:

.. code-block:: python

   from treeschema import TreeSchema
   ts = TreeSchema('<your email>', '<your secret key>')

   # Get the source and target schemas
   src_schema = ts.data_store('My Source DS').schema('src.schema')
   tgt_schema = ts.data_store('Target DS').schema('tgt.schema')

   transform_links = []

   # For each field in the source, check if the field name exists in the target
   for src_field in src_schema.fields.values():
      tgt_field = tgt_schema.field(src_field.name)
      if tgt_field:
         link_tuple = (src_field, tgt_field)
         transform_links.append(link_tuple)

   # Set the state to the current value, this will create new links and 
   # deprecate old links
   
   t = ts.transformation('My Second Transform')
   t.set_links_state(transform_links)

This can create a many links at once. For example, the output of 
a single high-volume transformation can be seen in Tree Schem here:

   .. image:: ../imgs/many_links.png
    :width: 700


Field Name Changes
------------------

Similarly, if the ETL/ELT process does change the field name then we can 
simply manually define a mapping to define where are fields come from 
and where they are going.

.. code-block:: python

   from treeschema import TreeSchema
   ts = TreeSchema('<your email>', '<your secret key>')

   # Get the source and target schemas
   src_schema = ts.data_store('My Source DS').schema('src.schema2')
   tgt_schema = ts.data_store('Target DS').schema('tgt.schema2')

   # key = source field name, value = target field name
   src_tgt_map = {
      'field_1': 'field_1a',
      'field_2': 'field_2b',
      'field_3': 'field_3a'
   }

   transform_links = []

   # For each field in the source, check if the field name exists in the target
   for src_field in src_schema.fields.values():
      tgt_field = tgt_schema.field(src_tgt_map[src_field.name])
      link_obj = (src_field, tgt_field)
      transform_links.append(link_obj)

   # Set the state to the current value, this will create new links and 
   # deprecate old links
   my_transform = {'name': 'My Third Transform', 'type': 'pub_sub_event'}
   t = ts.transformation(my_transform)
   t.set_links_state(transform_links)

Now within Tree Schema there is a mapping from one field to another:

   .. image:: ../imgs/multi_link_mapping.png
    :width: 700


