# FormS
FormS is a scalable spreadsheet formula execution engine. It allows users to directly adopt spreadsheet formlae to analyze dataframe data. It can also empower any spreadsheet interfaces with formula calculation capabilies. 

## FormS API

FormS API now includes two functions: forms.config and forms.compute_formula. 

`forms.config` allow users to config the formula execution, such as the number of cores used to compuate formulae. 

`forms.compute_formula` takes a dataframe and a formula string as inputs, and computes a new row or column of values using the [autofill semantics](https://support.microsoft.com/en-us/office/switch-between-relative-absolute-and-mixed-references-dfec08cd-ae65-4f56-839e-5f0d8d0baca9) of spreadsheet systems. Specifically, it automatically extrapolates a new row or column of formula strings from the input formula string based on [spreadsheet reference semantics](https://support.microsoft.com/en-us/office/switch-between-relative-absolute-and-mixed-references-dfec08cd-ae65-4f56-839e-5f0d8d0baca9) and computes this new column or row of formulae for an input dataframe. `forms.compute_formula` currently only supports fills down a column of values. 

Here is an illustrating example from Google sheets where the user writes the formula string and the others are automatically filled:

<img src="https://github.com/totemtang/forms/blob/main/resources/spreadsheet-example.png"  width="400" height="200">

## Demo
Please check out the demo here:

<img src="https://github.com/totemtang/forms/blob/main/resources/demo.gif">
