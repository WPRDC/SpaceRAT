
===============
Useful Commands
===============

.. code-block:: shell

     spacerat build-geo-indices blockgroup county county-subdivision neighborhood parcel school-district state-house state-senate tract us-house --yes

.. code-block:: shell

    spacerat populate-maps  \
             property-assessments \
             neighborhood county-subdivision county \
             -i lotarea \
             -i saledate \
             -i countybuilding -i countyland -i countytotal \
             -i localbuilding -i localland -i localtotal \
             -i fairmarketbuilding -i fairmarketland -i fairmarkettotal \
             -i countyexemptbldg \
             -i totalrooms \
             -i bedrooms \
             -i fullbaths \
             -i halfbaths \
             -i finishedlivingarea \
             --yes
