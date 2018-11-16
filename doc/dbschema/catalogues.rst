.. _doc-prep-catalogues:

Preparing catalogues
====================

Catalogue files should be provided as FITS tables.

.. note::
    PostGres database column names are typically case-insensitive; however,
    FITS table column names tend to be case-sensitive.

..
    Some use cases of this software may not require all of the database columns
    listed below. However, the existence of these columns is a requirement to
    keep both :any:`taipandb` and :any:`taipan` operational. Therefore, if a
    column is not required, it is recommended that you place a suitable dummy
    value in that column. It would also be necessary to check the priority
    computation function you are using (the main ones may be found in
    :any:`taipan.simulate.logic`) to make
    sure this dummy value does not cause unexpected behaviour. Alternatively,
    you could write your own priority computation funtion which only looks at the
    database information you are interested in.

Current ingest mapping
----------------------

Science targets
+++++++++++++++

At the moment, an extra ``if`` statement needs to be added to
:any:`loadScience` for each different catalogue that is attemped to be
ingested. In the interests of moving towards a fixed format for Taipan
catalogues, the following schema denotes the latest catalogue-to-database
matching.

This ingest mapping is correct for the input catalogue
`mock1.fits`.

+------------------------------+-----------------------------------------------+
| Database column              | Catalogue column                              |
+==============================+===============================================+
| ``target_id``                | ``target_id``                                 |
+------------------------------+-----------------------------------------------+
| ``ra``                       | ``ra``                                        |
+------------------------------+-----------------------------------------------+
| ``dec``                      | ``dec``                                       |
+------------------------------+-----------------------------------------------+
| ``is_science``               | Set to ``True`` everywhere                    |
+------------------------------+-----------------------------------------------+
| ``is_standard``              | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``is_guide``                 | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``ux``, ``uy`` and ``uz``    | Auto-computed during ingest                   |
+------------------------------+-----------------------------------------------+
| ``mag``                      | ``mag_J``                                     |
+------------------------------+-----------------------------------------------+
| ``is_h0_target``             | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``is_vpec_target``           | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``zspec``                    | ``z_obs``                                     |
+------------------------------+-----------------------------------------------+
| ``col_gi``                   | ``gminusi_AB``                                |
+------------------------------+-----------------------------------------------+
| ``mag_j``                    | ``mag_J``                                     |
+------------------------------+-----------------------------------------------+
| ``col_jk``                   | ``col_JK``, or -99 if ``NaN``                 |
+------------------------------+-----------------------------------------------+
| ``ebv``                      | ``ebv``                                       |
+------------------------------+-----------------------------------------------+
| ``glat``                     | ``glat``                                      |
+------------------------------+-----------------------------------------------+
| ``is_nir``                   | ``is_nir``                                    |
+------------------------------+-----------------------------------------------+
| ``is_lrg``                   | ``is_lrg``                                    |
+------------------------------+-----------------------------------------------+
| ``is_iband``                 | ``is_iband``                                  |
+------------------------------+-----------------------------------------------+
| ``is_lowz_target``           | ``hurry``                                     |
+------------------------------+-----------------------------------------------+
| ``is_sdss_target``           | ``is_sdss_legacy_target``                     |
+------------------------------+-----------------------------------------------+
| ``is_prisci_vpec_target``    | ``is_prisci_vpec_target``                     |
+------------------------------+-----------------------------------------------+
| ``is_full_vpec_target``      | ``is_vpec_target``                            |
+------------------------------+-----------------------------------------------+
| ``has_sdss_zspec``           | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``success``                  | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``ancillary_flags``          | Set to ``0`` everywhere                       |
+------------------------------+-----------------------------------------------+
| ``ancillary_priority``       | Set to ``0`` everywhere                       |
+------------------------------+-----------------------------------------------+

Guide targets
+++++++++++++

This schema is applied regardless of the guides catalogue file provided.

+------------------------------+-----------------------------------------------+
| Database column              | Catalogue column                              |
+==============================+===============================================+
| ``target_id``                | ``ucacid``                                    |
+------------------------------+-----------------------------------------------+
| ``ra``                       | ``raj2000``                                   |
+------------------------------+-----------------------------------------------+
| ``dec``                      | ``dec2000``                                   |
+------------------------------+-----------------------------------------------+
| ``is_science``               | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``is_guide``                 | Set to ``True`` everywhere                    |
+------------------------------+-----------------------------------------------+
| ``is_sky``                   | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``is_standard``              | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``ux``, ``uy``, ``uz``       | Auto-computed at ingest                       |
+------------------------------+-----------------------------------------------+

Standard targets
++++++++++++++++

This schema is applied regardless of the guides catalogue file provided.

+------------------------------+-----------------------------------------------+
| Database column              | Catalogue column                              |
+==============================+===============================================+
| ``target_id``                | ``ucacid``                                    |
+------------------------------+-----------------------------------------------+
| ``ra``                       | ``raj2000``                                   |
+------------------------------+-----------------------------------------------+
| ``dec``                      | ``dec2000``                                   |
+------------------------------+-----------------------------------------------+
| ``is_science``               | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``is_guide``                 | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``is_sky``                   | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``is_standard``              | Set to ``True`` everywhere                    |
+------------------------------+-----------------------------------------------+
| ``ux``, ``uy``, ``uz``       | Auto-computed at ingest                       |
+------------------------------+-----------------------------------------------+

Sky targets
++++++++++++++++

This schema is applied regardless of the sky catalogue file provided.

+------------------------------+-----------------------------------------------+
| Database column              | Catalogue column                              |
+==============================+===============================================+
| ``target_id``                | ``ucacid``                                    |
+------------------------------+-----------------------------------------------+
| ``ra``                       | ``raj2000``                                   |
+------------------------------+-----------------------------------------------+
| ``dec``                      | ``dec2000``                                   |
+------------------------------+-----------------------------------------------+
| ``is_science``               | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``is_guide``                 | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``is_sky``                   | Set to ``True`` everywhere                    |
+------------------------------+-----------------------------------------------+
| ``is_standard``              | Set to ``False`` everywhere                   |
+------------------------------+-----------------------------------------------+
| ``ux``, ``uy``, ``uz``       | Auto-computed at ingest                       |
+------------------------------+-----------------------------------------------+
