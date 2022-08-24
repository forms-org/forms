# FormS
FormS is a scalable spreadsheet formula execution engine. It allows users to directly adopt spreadsheet formlae to analyze dataframe data. It can also empower any spreadsheet interfaces with formula calculation capabilies. 

## FormS API

FormS API now includes two functions: `forms.config` and `forms.compute_formula`. 

`forms.config` allow users to config the formula execution, such as the number of cores used to compuate formulae. 

`forms.compute_formula` takes a dataframe and a formula string as inputs, and computes a new row or column of values using the [autofill semantics](https://support.microsoft.com/en-us/office/switch-between-relative-absolute-and-mixed-references-dfec08cd-ae65-4f56-839e-5f0d8d0baca9) of spreadsheet systems. Specifically, it automatically extrapolates a row or column of formula strings from the input formula string based on [spreadsheet reference semantics](https://support.microsoft.com/en-us/office/switch-between-relative-absolute-and-mixed-references-dfec08cd-ae65-4f56-839e-5f0d8d0baca9), and computes the values for the new column or row of formulae for the input dataframe. `forms.compute_formula` currently only supports filling down a column of values. 

Below is an illustrating example: the user writes the formula string `SUM(A1:B2)` at cell `C1` and `C2:C7` are automatically filled:

<img src="https://github.com/totemtang/forms/blob/main/resources/spreadsheet-example.png"  width="400" height="200">

## Demo
<img src="https://github.com/totemtang/forms/blob/main/resources/demo.gif">
