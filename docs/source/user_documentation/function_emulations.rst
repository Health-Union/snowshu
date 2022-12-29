.. raw:: html

   <style type="text/css">
     table {
       table-layout: fixed;
       width: 100%
     }
     html.writer-html5 .rst-content table.docutils th>p {
       text-align: center;
     }
     html.writer-html5 .rst-content table.docutils td>p {
       font-size: 10px;
       text-align: left;
     }
     .rst-content table.docutils td {
       padding-left:8px;
     }
     .rst-content blockquote, blockquote ~p {
       margin-left: 0;
       margin-bottom: 5px;
       line-height: 0;
       font-size: 10px;
       white-space: pre-line;
     }
   </style>

===================
Function Emulations
===================

To make SnowShu replicas behave as much like their source counterparts as possible, functions are applied to *emulate* them. 
Each source-to-target relationship has a unique set of emulations, cataloged here.

-----------------------
Snowflake DSL Emulation
-----------------------

.. csv-table:: Snowflake DSL Emulation   
   :file: ./snowflake_function_map.csv
   :widths: 20, 11, 10, 10
   :header-rows: 1
