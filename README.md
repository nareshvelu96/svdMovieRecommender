SVD Item Similarity Engine Template

Template to calculate similarity between items based on their attributes. 
Attributes can be either numeric or categorical in the last case it will be 
encoded using one-hot encoder. Algorithm uses SVD in order to reduce data 
dimensionality. Cosine similarity is now implemented but can be easily 
extended to other similarity measures.

This template is a simple example of unsupervised learning algorithm 
implementation. Given items with their attributes of numeric and string types
(like movie release year, duration, director, actors, etc.) we can measure 
similarity between items based on some metric (cosine similarity in this 
example). 

