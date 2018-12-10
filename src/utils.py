#!/usr/bin/env python
# -*- coding: utf-8 -*-

import networkx as nx
import asyncio
import numpy as np
import pandas as pd
import glob
import pymongo as py
from bson.objectid import ObjectId
import os

btwns_dict = None
closeness_dict = None


def readTriples(path="data/glosário/",preprocess=False):
	"""
	Read all the .nt files inside the directory. If preprocess=True,
	objetify all the literals on the data.
	"""
	
	def readTriplesFile(path="data/triples.nt"):
		triples = []
		with open(path) as f:
			for line in f:
				# line = line.replace(">  <", "> <")
				# triples = triples  + [line.split(" ")[0:-1]]
				line = line[:-3] if line.endswith("\n") else line[:-2]
				values = line.split("> ")

				src = values[0]+">"
				uri = values[1][1:]
				trg = "> ".join(values[2:])

				triples += [[src,uri,trg]]

		return  triples 

	def preProcess(file):
		dictionary_of_literal = {}
		dictionary_of_classes = {}
		array = []
		with open(file,encoding="utf-8") as f:
			for line in f:
				line = line.replace(">  <","> <")
				literal = line.split(" ")[2]
				predicate = line.split(" ")[1]
				nameOf = None
				
				if "<" != literal[0]:

					nameOf = predicate.split("/")[-1].split("#")[-1]

					if literal not in dictionary_of_literal.keys():
						dictionary_of_literal[literal] = {}
						dictionary_of_literal[literal][predicate] = "<http://petrobras.com.br/ontology/mapa-exp/"+nameOf.lower().replace(">","")+'/'+str(ObjectId())+'>'				
						triple_of_Object =  ["<http://www.petrobras.com.br/ontology/petro#"+nameOf[0].upper()+nameOf[1:-1]+'>',"<http://www.w3.org/2000/01/rdf-schema#subClassOf>","<http://www.w3.org/2004/02/skos/core#Concept> ."]
						triple_of_type = [dictionary_of_literal[literal][predicate],"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>","<http://www.petrobras.com.br/ontology/petro#"+nameOf[0].upper()+nameOf[1:-1]+"> ."]

						array.append(triple_of_type)
						array.append(triple_of_Object)

					else:
						if predicate not in dictionary_of_literal[literal].keys():
							dictionary_of_literal[literal][predicate] = "<http://petrobras.com.br/ontology/mapa-exp/"+nameOf.lower().replace(">","")+'/'+str(ObjectId())+'>'
							triple_of_Object =  ["<http://www.petrobras.com.br/ontology/petro#"+nameOf[0].upper()+nameOf[1:-1]+'>',"<http://www.w3.org/2000/01/rdf-schema#subClassOf>","<http://www.w3.org/2004/02/skos/core#Concept> ."]
							triple_of_type = [dictionary_of_literal[literal][predicate],"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>","<http://www.petrobras.com.br/ontology/petro#"+nameOf[0].upper()+nameOf[1:-1]+"> ."]
							
							array.append(triple_of_type)
							array.append(triple_of_Object)

					triple_of_reference = [dictionary_of_literal[literal][predicate],predicate,literal]
					triples_of_relation =[line.split(" ")[0],predicate,dictionary_of_literal[literal][predicate]]  

					array.append(triples_of_relation)
					array.append(triple_of_reference)

				#do nothing , is not a literal
				else:
					array.append(line.split(" ")[0:-1])

		return array

	triples = [] 

	if preprocess:
		reader = preProcess
	else:
		reader = readTriplesFile

	for filename in glob.iglob(path+'**/*.nt', recursive=True):
		triples = triples + reader(filename)
	with open("glossarioTriples.txt",'w',encoding="utf-8") as g:
		for triple in triples:
			g.write(triple[0]+' '+triple[1]+' '+triple[2]+"\n")

	return triples

def generateGraph(triples):
	"""
	Generate a networkx graph object from a list of n-triples.
	"""
	
	def areConnected(t1,t2):
		return t1!=t2 and (t1[0]==t2[0] or t1[2]==t2[2] or t1[0]==t2[2] or t1[2]==t2[0])

	graph =  nx.DiGraph()
	predicatesByURI = {}
	for t in triples:
		#graph.add_node(t1[1])
		graph.add_edge(t[0],t[2])
		if t[0] not in predicatesByURI:
			predicatesByURI[t[0]] = {}
		if t[2] not in predicatesByURI[t[0]]:
			predicatesByURI[t[0]][t[2]] = []
		predicatesByURI[t[0]][t[2]].append(t[1][1:-1])

	return graph,predicatesByURI

def node_betweeness_centrality(nx_graph,dict=False):
	"""
	Calculate the betweenness centrality for the nodes
	on the graph.
	"""
	bt_result = nx.betweenness_centrality(nx_graph)

	if not dict:
		bt_result = [(node,bt_result[node]) for node in sorted(bt_result, key=lambda v: bt_result[v])]

	return bt_result

def predicates_betweeness_centrality(nx_graph,predicatesByURI):
	"""
	Calculate the betweenness centrality for the predicates
	on the graph. This one uses a native edge-betweenness implementation from networkx
	"""
	bt_result = nx.edge_betweenness_centrality(nx_graph)

	bt_Dict= {}
	for p,result in [(predicatesByURI[b[0]][b[1]],bt_result[b]) for b in bt_result] :
		for b in p:
			try :
				bt_Dict[b] += result
			except :
				bt_Dict[b] = 0.

	bt_result_sorted = sorted(bt_Dict,key=lambda value: bt_Dict[value])

	return [(p,bt_Dict[p]) for p in bt_result_sorted]

def predicates_betweeness_centrality_distributed(nx_graph, predicates):
	"""
	Calculate the betweenness centrality for the predicates
	on the graph. This one uses our distributed implementation from nodes centrality
	"""
	betweeness = node_betweeness_centrality(nx_graph,True)
	return distribute_relevance_to_predicates(betweeness,predicates)

def node_closeness_centrality(nx_graph, dict=False):
	"""
	Calculate the closeness centrality for the nodes
	on the graph.
	"""
	closeness = nx.closeness_centrality(nx_graph)

	if not dict :
		closeness =  [(node,closeness[node]) for node in sorted(closeness, key=lambda v: closeness[v])]

	return closeness

def predicates_closeness_centrality(nx_graph, predicates):
	"""
	Calculate the closeness centrality for the nodes
	on the graph and distribute then to the predicates
	"""
	closeness = node_closeness_centrality(nx_graph,True)
	return distribute_relevance_to_predicates(closeness,predicates)

def node_eigenvectors_centrality(nx_graph,dict=False):
	"""
	Calculate the eigenvectors centrality for the nodes
	on the graph.
	"""
	eig_cent = nx.eigenvector_centrality(nx_graph,tol=1e-05)

	if not dict:
		eig_cent = [(node,eig_cent[node]) for node in sorted(eig_cent, key=lambda v: eig_cent[v])]

	return eig_cent

def predicates_eigenvectors_centrality(nx_graph, predicates):
	"""
	Calculate the eigenvectors centrality for the nodes
	on the graph and distribute then to the predicates
	"""
	eigenvectors = node_eigenvectors_centrality(nx_graph,True)
	return distribute_relevance_to_predicates(eigenvectors,predicates)

def node_katz_centrality(nx_graph,dict=False):
	"""
	Calculate the katz centrality for the nodes
	on the graph.
	"""
	katz_cent = nx.katz_centrality(nx_graph,max_iter=2000,tol=1e-03)

	if not dict:
		katz_cent = [(node,katz_cent[node]) for node in sorted(katz_cent, key=lambda v: katz_cent[v])]

	return katz_cent

def predicates_katz_centrality(nx_graph, predicates):
	"""
	Calculate the katz centrality for the nodes
	on the graph and distribute then to the predicates
	"""
	katz = node_katz_centrality(nx_graph,True)
	return distribute_relevance_to_predicates(katz,predicates)

def node_page_rank(nx_graph,dict=False):
	"""
	Calculate the page rank value for the nodes
	on the graph.
	"""
	page_rank = nx.pagerank(nx_graph)

	if not dict:
		page_rank = [(node,page_rank[node]) for node in sorted(page_rank, key=lambda v: page_rank[v])]

	return page_rank

def predicates_page_rank(nx_graph, predicates):
	"""
	Calculate the page rank values for the nodes
	on the graph and distribute then to the predicates
	"""
	page_rank = node_page_rank(nx_graph,True)
	return distribute_relevance_to_predicates(page_rank,predicates)

def distribute_relevance_to_predicates(centrality,predicatesByURI):
	"""
	Distribute the values from nodes centralities to the predicates.

	Parameters
	----------
	centrality : dict(str : float)
		Dictionary of terms and their centrality values

	predicatesByURI : dict(str : dict(str: list(str)))
		Dictionary of predicates between the first and second keys of the dictionary.
		ex: p[t1][t2] is a list of predicates that occurs between t1 and t2 (in this order) 
	"""
	predicatesFrequence = {}
	for u in centrality:
		if u in predicatesByURI:	
			for v in predicatesByURI[u]:
					for r in predicatesByURI[u][v]:
						if r in predicatesFrequence:
							if u in predicatesFrequence[r]:
								predicatesFrequence[r][u] += 1
							else:
								predicatesFrequence[r][u] = 1	 
						else:
							predicatesFrequence[r] = {}
							predicatesFrequence[r][u] = 1

	centrality_predicates = {}
	total = 0 
	for z in predicatesFrequence:
		total += sum(x for x in predicatesFrequence[z].values())

	for r in predicatesFrequence :
		centrality_predicates[r] = 0
		for u in predicatesFrequence[r]: 	
				centrality_predicates[r] += predicatesFrequence[r][u] * centrality[u]

		centrality_predicates[r] = centrality_predicates[r] / total

	return [(predicate,centrality_predicates[predicate]) for predicate in sorted(centrality_predicates, key=lambda v: centrality_predicates[v])]	

def analyse_ontology_predicates(path,centrs,resultsPath="results",objetify=False):
	"""
	Analyse the ontology stored in a path, applying the centralities desired and saving on the results
	directory. If objetify = True, it creates a new class for all the literals, new class instance and 
	make them all point to the literal. This should be used on ontologies that a literal repeats to lot
	of terms.

	Parameters
	----------
	path : str
		Path to the folder that contains (recursively) the .nt triples files.

	centrs : list(str)
		List of the centralities that will be applied over the predicates from the ontology.

	resultsPath : str
		Path to the results folder that will store the results.

	objetify : bool
		Flag to do the objetification of the ontology literals.

	Returns
	-------
	pandas.DataFrame
		A data frame from pandas where each line represents a predicate and the column represents
		a centrality.
	"""
	resultsPath+="/predicates"
	if not os.path.exists(resultsPath):
		os.makedirs(resultsPath)

	triples = readTriples(path,objetify)
	print("{} triples loaded from {}".format(len(triples),path))

	graph,predicates = generateGraph(triples)

	values = {}
	index = ["predicates"]
	for centrality in centrs:
		index.append(centrality)
		print("Calculating: {}".format(centrality))
		ct = predicates_centralities[centrality](graph,predicates)
		with open("{}/{}.txt".format(resultsPath,centrality),'w') as f:
			print("Saving results from {}".format(centrality))
			for p,c in ct:
				f.write(p+"\t"+str(c)+"\n")
				if not p in values:
					values[p] = []
				values[p].append(c)

	with open("{}/predicatesDataFrame.csv".format(resultsPath),'w') as f:
		f.write(",".join(index)+"\n")
		for p in values:
			f.write("{},{}".format(p,",".join([str(val) for val in values[p]])+"\n"))

	return pd.read_csv("{}/predicatesDataFrame.csv".format(resultsPath))

def analyse_ontology_nodes(path,centrs,resultsPath="results"):
	"""
	Analyse the ontology stored in a path, applying the centralities desired and saving on the results
	directory.

	Parameters
	----------
	path : str
		Path to the folder that contains (recursively) the .nt triples files.

	centrs : list(str)
		List of the centralities that will be applied over the predicates from the ontology.

	resultsPath : str
		Path to the results folder that will store the results.

	Returns
	-------
	pandas.DataFrame
		A data frame from pandas where each line represents a subject/object and the column represents
		a centrality.
	"""

	resultsPath+="/nodes"
	if not os.path.exists(resultsPath):
		os.makedirs(resultsPath)

	triples = readTriples(path)
	print("{} triples loaded from {}".format(len(triples),path))

	graph,_ = generateGraph(triples)

	values = {}
	index = ["terms"]
	for centrality in centrs:
		index.append(centrality)
		print("Calculating: {}".format(centrality))
		ct = nodes_centralities[centrality](graph)
		with open("{}/{}.txt".format(resultsPath,centrality),'w') as f:
			print("Saving results from {}".format(centrality))
			for n,c in ct:
				f.write(n+"\t"+str(c)+"\n")
				if not n in values:
					values[n] = []
				values[n].append(c)

	with open("{}/nodesDataFrame.tsv".format(resultsPath),'w') as f:
		f.write("\t".join(index)+"\n")
		for n in values:
			f.write("{}\t{}".format(n,"\t".join([str(val) for val in values[n]])+"\n"))

	return pd.read_csv("{}/nodesDataFrame.tsv".format(resultsPath),delimiter="\t")


predicates_centralities = {
	"closeness":predicates_closeness_centrality,
	"eigenvectors":predicates_eigenvectors_centrality,
	"betweeness":predicates_betweeness_centrality,
	"betweeness_by_mean":predicates_betweeness_centrality_distributed,
	"katz":predicates_katz_centrality,
	"page_rank":predicates_page_rank
}

nodes_centralities = {
	"closeness":node_closeness_centrality,
	"eigenvectors":node_eigenvectors_centrality,
	"betweeness":node_betweeness_centrality,
	"katz":node_katz_centrality,
	"page_rank":node_page_rank
}
