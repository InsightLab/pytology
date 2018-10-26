from src import utils as ut
import os
import time
import argparse

os.system("clear")



parser = argparse.ArgumentParser()
parser.add_argument("dataPath",type=str,help="Path to the data folder that contains files (and folders with files) with .nt extension.")
parser.add_argument("-r",type=str,help="Path to results folder.")
parser.add_argument("-o",action="store_true",help="Use to objetify the literals from the data (used only on predicates analisis).")
parser.add_argument("-p",action="store_true",help="Apply centralities to predicates")
parser.add_argument("-n",action="store_true",help="Apply centralities to nodes (objects/terms)")
parser.add_argument("-c",choices=list(ut.predicates_centralities.keys()),help="Set the centralities that will be calculated on your data.", type=str, nargs="+", default=[])
args = parser.parse_args()

if not args.r:
	args.r="results"
elif args.r[-1]=='/':
	args.r = args.r[0:-1]

if not os.path.exists(args.r):
    os.makedirs(args.r)

if(len(args.c)!=0):
	print("Configurations:")
	print("Data path: {}".format(args.dataPath))
	print("Results path: {}".format(args.r))
	print("Centralities to calculate: {}".format(','.join([c for c in args.c])))
	print("Objetify? {}".format(args.o))

	if(args.p):
		start_time = time.time()

		print("\n\nAnalysing predicates...")
		df = ut.analyse_ontology_predicates(args.dataPath,args.c,args.r,args.o)
		#print(df)

		elapsed_time = time.time() - start_time
		print("\n\n",elapsed_time/60,"minutes")

	if(args.n):
		start_time = time.time()

		print("\n\nAnalysing nodes...")
		if "betweeness_by_mean" in args.c:
			args.c.remove("betweeness_by_mean")
		df = ut.analyse_ontology_nodes(args.dataPath,args.c,args.r)
		#print(df)

		elapsed_time = time.time() - start_time
		print("\n\n",elapsed_time/60,"minutes")

	# print(df.corr(method='spearman'))
	# print(df.corr(method='pearson'))
	# print(df.corr(method='kendall'))

	# methods = ['spearman','pearson','kendall']
	# with open("{}/rank_correlations.txt".format(args.r),'w') as f:
	# 	for method in methods:
	# 		f.write('--------------------- '+method+' results ----------------------------\n')
	# 		f.write(str(df.corr(method=method)))
	# 		f.write('\n---------------------------------------------------------------------\n')



