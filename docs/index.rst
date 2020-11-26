
.. meta::
    :description lang=en:
        The Tree Schema Python client allows you to interact with your data 
        catalog programmatically, including managing your data lineage as code.


Tree Schema Python Client
=========================

Interacting with Tree Schema through the API provides many benefits and allows:

- Developers to manage data lineage as code
- Data engineers to wire CICD pipelines that automatically push changes to Tree Schema
- Data scientists to explore the data catalog in the same notebook for data analysis

Table of Contents:
==================

.. toctree::
   :maxdepth: 1

   self
   secret_key/secret_key.rst
   examples/examples.rst
   api/api.rst
   
   

Installation
------------

::

   pip install treeschema

Using the library
-----------------

This libarary is a light wrapper around the Tree Schema REST API. The 
library allows you to explore your data catalog as well as to create or 
update your data assets programmatically.

One of the most powerful features is that you can manage your data lineage 
as code, which can be done as simply as:

.. code-block:: python

   from treeschema import TreeSchema
   ts = TreeSchema('<your email>', '<your secret key>')

   # Define the schemas that have at least 1 field in the transformation
   dvc_session_schema = ts.data_store('Kafka Cluster').schema('dvc.session:v1:avro')
   user_click_schema = ts.data_store('Kafka Cluster').schema('user.clickstream:v1:avro')

   user_mongo_schema = ts.data_store('Mongo DB - User Events').schema('user.session')

   sess_analytics_schema = ts.data_store('Redshift Analytics').schema('user.session')
   click_analytics_schema = ts.data_store('Redshift Analytics').schema('user.page_view')

   # Create or retrieve the transformation
   transform_inputs = {'name': 'Python Transform!', 'type': 'pub_sub_event'}    
   t = ts.transformation(transform_inputs)

   transform_links = [
        (dvc_session_schema.field('session_id'), sess_analytics_schema.field('session_id')),
        (dvc_session_schema.field('event_ts'), sess_analytics_schema.field('event_ts')),

        (dvc_session_schema.field('user_id'), user_click_schema.field('user_id')),
        (dvc_session_schema.field('event_ts'), user_click_schema.field('event_ts')),

        (user_click_schema.field('user_id'), user_mongo_schema.field('user_id')),
        (user_click_schema.field('event_ts'), user_mongo_schema.field('event_ts')),

        (user_click_schema.field('user_id'), click_analytics_schema.field('user_id')),
        (user_click_schema.field('page_id'), click_analytics_schema.field('page_id')),
        (user_click_schema.field('event_ts'), click_analytics_schema.field('event_ts'))
   ]

   t.create_links(transform_links) 


This create the following data lineage, which you can explore in Tree Schema.

   .. image:: ./imgs/links_example.png
    :width: 700


Check out all of the tutorials and walkthroughs in the **Examples** section!


Other Tree Schema Articles & Tutorials
--------------------------------------

- `How to Use Tree Schema 101 <https://treeschema.com/blog/tree-schema-101/>`_
- `Tree Schema REST API documentation <https://developer.treeschema.com/rest-api>`_
- `Tree Schema Data Lineage Knowledge Base <https://help.treeschema.com/catalog/lineage/lineage.html>`_

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

