# Homework 2 - Hadoop MapReduc Algorithms

**Given two input files:**

### 1) userdata.txt

This file contains relevant information for a given user. 
The file contains userId, name, address and date of birth.

0,Evangeline,Taylor,3396 Rogers Street,Loveland,Ohio,45140,US,Unfue1996,1/24/1996

1,Robert,Cottrell,1775 Sycamore Circle,Dallas,Texas,75234,US,Dinvis,11/26/1973

2,Kathryn,Winn,2858 Jerry Dove Drive,Wampee,South Carolina,29568,US,Couchisem,9/29/1941

3,Harry,Kowalewski,3835 Romrog Way,Indianola,Nebraska,69034,US,Scather,5/21/1950

4,Paula,McCoy,4915 Aaron Smith Drive,York,Pennsylvania,17404,US,Thandsoll,10/28/1935

5,Beth,Watson,2149 Powder House Road,Lake Worth,Florida,33463,US,Aullewon,8/27/1970

6,Mark,Berman,1107 Roane Avenue,Houston,Texas,77032,US,Latme1978,8/21/1978

7,David,Brown,3427 Tuna Street,Pontiac,Michigan,48342,US,Applay,12/23/1970

8,Sergio,McDonald,2323 Rafe Lane,Memphis,Mississippi,38104,US,Evered,11/29/1938

9,Jaime,Ramirez,877 Isaacs Creek Road,Springfield,Illinois,62701,US,Loped1991,5/10/1991

...

### 2) soc-LiveJournal1Adj.txt

The second file contains a userID with all of their friends userID.


0	

1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94


1	

0,5,20,135,2409,8715,8932,10623,12347,12846,13840,13845,14005,20075,21556,22939,23520,28193,29724,29791,29826,30691,31232,31435,32317,32489,34394,35589,35605,35606,35613,35633,35648,35678,38737,43447,44846,44887,49226,49985,623,629,4999,6156,13912,14248,15190,17636,19217,20074,27536,29481,29726,29767,30257,33060,34250,34280,34392,34406,34418,34420,34439,34450,34651,45054,49592


2

0,117,135,1220,2755,12453,24539,24714,41456,45046,49927,6893,13795,16659,32828,41878

3

0,12,41,55,1532,12636,13185,27552,38737

4

0,8,14,15,18,27,72,80,15326,19068,19079,24596,42697,46126,74,77,33269,38792,38822

5

0,1,20,2022,22939,23527,30257,32503,35633,41457,43262,44846,49574,31140,32828

6	

0,21,98,2203,3238,5040,8795,9843,9847,15294,17874,18286,18311,18320,20553,35699,35776,38736,38750,38800,543,575,11879,12682,14943,15283,18332,18560,18625,25247,33080,34412,35785,35822,42231

7

0,31993,40218,40433,1357,21843

8

0,4,38,46,72,85,24777,83,33380

9

0,6085,18972,1926

...

## Problem 1 - Finding Mutual Friends Between Two Users

Given two users I will output a non-empty list for users with mutual friends. The list will contain the userID's for the mutual friends they have.


## Problem 3 - Finding the Average Age for a Users Friends

In this solution I read the userdata.txt file in memory and store useful information from users into a lookup table. This lookup table will give user information when you pass a userID. 


The Lookup table will have the following implementation:
- Key         Value
- "0"				"24,Evangeline"
- "1"				"47,Robert"
- "2"				"70,Kathyrn"
- "3" 			"85,Paula"


A user will have a list of friends. With the list of friends I will use the lookup table to get the user's age and calculate the average of the users friend. 

For user 0:

0	

Has friends:

1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94


## Problem 4 - Sorting Friends List by Age

For user 49979:

Has friends:

49979	         

49896,49936,49949,49952,49963,49973,49951


Their friends list will be sorted by friends first name and age:

49979	Eric 	24

49979	Robert 	50

49979	Peggy 	59

49979	Donald 	71

49979	Deborah 	76

49979	Earnest 	78

49979	Nicholas 	87





