
Create a T2
===========

Basic structure
***************

T2 module inputs
################

The input of any T2 module is a :py:class:`ampel.Transient`

The typical methods of Transient are::
  
  get_radec(*args, **kwargs) # this could be a list of RA/Dec
  get_photopoint(*args, **kwargs) # Photopoint or list of
  get_t2results(*args, **kwargs) # queries already ran T2 module results

T2 module output
################

It returns a dictionary of key values (no object instances inside). That means,
you dictionaries must be serializable, so for simplicity, the output dictionary
should only contain native Python types (`float`, `int`, `string`, `list`,
`tuple`, `dict`).
