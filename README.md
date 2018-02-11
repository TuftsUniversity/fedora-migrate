# FedoraMigrate

Contains tasks for migrating Tufts Fedora 3 objects and collections to our hyrax baed Fedora 4 applications.

## requirements

* a collection_map.csv file which defines migration of F3 Collections
** TODO: needs EAD info

## tasks

* define_top_level_collections
** Defines the following collections:
*** 'Manuscripts','New Nation Votes','Test Items','Tufts Scholarship','University Archives','University Publications'
* define_secondary_collections
** adds collections to primary collections as defined in required csv
* delete_all_collections
** deletes every collection in the repository, probably overzealous and should be refactored.
* import_collections
** populates a sql table used for associating migrating objects with migrated collections.
