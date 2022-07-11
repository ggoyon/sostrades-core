The gridsearch discipline is a subclass imported from the DOE (design of experiments) discipline from GEMSEO. 

The gridsearch allows to find the input parameters which are optimal for a given output. It is a systematic exploration of the input parameters space.

E.g.: how to find the optimal RC/NRC parameters which optimize the cashflow ?

Gridsearch exploration evaluates several number of scenarii generating by combinations among some input variables selected. In fact, from a simple study, a user can chose some inputs, define a range of values they will take for each scenario generated and at the end chose the outputs to display at the end of the computation for each scenario.

The several usage steps are detailed below.

## Step 1 : Inputs/Outputs selection

In order to start, the first step is to select the inputs that will change for each scenario and the output to be displayed at the end of a run.

![1](GS_step1.PNG)

From a simple study, the user has to select the inputs/outputs to be evaluated. Notice as a first version, it is allowed to select only 3 variables from the possible float/int values inputs. 

Concerning the outputs, the user can select only one for the moment. This variable must have only one row by scenario to be displayed on the outputs graphs after running.

![1.2](GS_step2.PNG)
![1.3](GS_step5.PNG)

At the end of the selection, the user needs to configure a first time in order to continue the next steps.

## Step 2 : Design space definition

Once the configuration is done, a new input appears named "Design space". The design space allows to define the range of values for each selected input. It will lead to the scenarii generation with all samples combinations. 

![2.1](GS_step3.PNG)

For each selected input, the user can define the lower/upper bond for the range of values they will take as well as the number of points it will peak on this range. As an example, if the range is [0,100] and number of points is 3, the values for this variable will be 0,50,100. If the number of points is 1 then the variable value will be 50.

Notice the range is set by default as [0,100] and the number of points as 2 (must be at least 1).

![2.2](GS_step4.PNG)

Once te evaluated inputs/outputs and design space are set. The study can be run.

## Step 3 : Outputs generation

![2.2](GS_step7.PNG)

At the end of the computation, two outputs are generated. A "Samples" output summarize the scenarii created with the name of the scenario and the inputs associated. For each selected outputs in the "Evaluated Outputs" (only one for this actual version), a dict is created with output values for each scenario.

![2.2](GS_step8.PNG)

## Step 4 : Postprocessing

The graphs are contour plots for each identified float columns in the output dict generated (only for the cases where there is a unique value per scenario [^1]). These outputs are contourplotted for 2 or 3 inputs maximum. In the case of 3 inputs selected, a slicer is set in order to show a 3 dimensions contourplot graph.

[^1]: if there is no suitable column within the output dict generated (*type float & one value per scenario*), no contourplot graph will be displayed. 


![2.2](GS_step9.PNG)