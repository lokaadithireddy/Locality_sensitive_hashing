import numpy as np
import pandas as pd
import timeit
from pyspark import SparkContext
from pyspark import SQLContext
from PIL import Image
import matplotlib.pyplot as plt
#from pyspark.sql import Row

class LSH:

    def __init__(self, filename, k, L):
        """
        Initializes the LSH object
        filename - name of file containing dataframe to be searched
        k - number of thresholds in each function
        L - number of functions
        """
        # do not edit this function!
        self.sc = SparkContext()
        self.k = k
        self.L = L
        self.A = self.load_data(filename)
        self.functions = self.create_functions()
        self.hashed_A = self.hash_data()
        
    # TODO: Implement this
    def l1(self, u, v):
        """
        Finds the L1 distance between two vectors
        u and v are 1-dimensional Row objects
        """
        p=np.sum(abs(np.array(u)-np.array(v)))
        return p
        #raise NotImplementedError

    # TODO: Implement this
    def load_data(self, filename):
        """
        Loads the data into a spark DataFrame, where each row corresponds to
        an image patch -- this step is sort of slow.
        Each row in the data is an image, and there are 400 columns.
        """
        dframe=self.sc.textFile(filename)
        return dframe
        #raise NotImplementedError
        
    # TODO: Implement this
    
    def create_function(self, dimensions, thresholds):
        """
        Creates a hash function from a list of dimensions and thresholds.
        """
        #listofvalues=[]
        def f(v):
            
            l=[]
            for dim,thre in zip(dimensions,thresholds):
                if v[dim]>=thre:
                    l.append("1")
                else:
                    l.append("0")
            return " ".join(l)
            #raise NotImplementedError
            
        return f
        
    def create_functions(self, num_dimensions=400, min_threshold=0, max_threshold=255):
        """
        Creates the LSH functions (functions that compute L K-bit hash keys).
        Each function selects k dimensions (i.e. column indices of the image matrix)
        at random, and then chooses a random threshold for each dimension, between 0 and
        255.  For any image, if its value on a given dimension is greater than or equal to
        the randomly chosen threshold, we set that bit to 1.  Each hash function returns
        a length-k bit string of the form "0101010001101001...", and the L hash functions 
        will produce L such bit strings for each image.
        """
        functions = []
        for i in range(self.L):
            dimensions = np.random.randint(low = 0, 
                                    high = num_dimensions,
                                    size = self.k)
            thresholds = np.random.randint(low = min_threshold, 
                                    high = max_threshold + 1, 
                                    size = self.k)

            functions.append(self.create_function(dimensions, thresholds))
        #print(functions)
        return functions
    

    # TODO: Implement this
    
    def hash_vector(self,v):
        """
        Hashes an individual vector (i.e. image).  This produces an array with L
        entries, where each entry is a string of k bits.
        """
        # you will need to use self.functions for this method
        fn=self.create_functions()
        
        data1=v.map(lambda w: [float(i) for i in w.split(",")])
        
        hashedval=data1.map(lambda x: (x,[f(np.array(x)) for f in fn]))
        return hashedval
      
        #raise NotImplementedError
    
    # TODO: Implement this
    def hash_data(self):
        """
        Hashes the data in A, where each row is a datapoint, using the L
        functions in 'self.functions'
        """
        
        fns=self.create_functions()
        
        data=self.A.map(lambda w: [float(i) for i in w.split(",")])
        
        dataWithIndex = data.zipWithIndex()
        
        hashed=dataWithIndex.map(lambda x: [x[1],x[0],[f(np.array(x[0])) for f in fns]])
        
        return hashed
        #raise NotImplementedError
    
    # TODO: Implement this
    def get_candidates(self,hashed_point, query_index):
        """
        Retrieve all of the points that hash to one of the same buckets 
        as the query point.  Do not do any random sampling (unlike what the first
        part of this problem prescribes).
        Don't retrieve a point if it is the same point as the query point.
        """
        # you will need to use self.hashed_A for this method

        candidates=self.hashed_A.filter(lambda x : np.any(np.array(x[2])==np.array(hashed_point))) 
        
        print(candidates.count())
        
        return candidates
    
        #raise NotImplementedError
      
    # TODO: Implement this
    def lsh_search(self,query_index, num_neighbors ):
        """
        Run the entire LSH algorithm
        """
        def l1(u, v):
            p=np.sum(abs(np.array(u)-np.array(v)))
            return p

        hashed_point = self.hashed_A.collect()[query_index]

        
        candidates=self.get_candidates(hashed_point[2], query_index)
        
        #print("hi7")
        
        distances_cand=candidates.map(lambda x : (l1(x[1],hashed_point[1]),x[0]))
        
        sorted_dist = distances_cand.sortByKey()
        
        #print(sorted_dist.take(4))
        
        return sorted_dist.take(16)[1:num_neighbors+1]
        

# Plots images at the specified rows and saves them each to files.
     
def plot(A, row_nums, base_filename):
    for row_num in row_nums:
        patch = np.reshape(A[row_num, :], [20, 20])
        im = Image.fromarray(patch)
        if im.mode != 'RGB':
            im = im.convert('RGB')
        im.save(base_filename + "-" + str(row_num) + ".png")
        

# Finds the nearest neighbors to a given vector, using linear search.
# TODO: Implement this

    
def linear_search(A, query_index, num_neighbors):
    distances=[]
    finall1=[]
    query_index_data= A.iloc[query_index]
    
    for i in range(0,len(A)):
        if(i==query_index):
            continue
        #print(lsh.l1(A.iloc[i],query_index_data))
        distances.append((i,lsh.l1(A.iloc[i],query_index_data)))
        
        
    near_neighbors=sorted(distances,key=lambda k: k[1])
    
    for p in near_neighbors[:num_neighbors]:
        finall1.append((p[0],p[1]))

    return finall1
    
    #raise NotImplementedError
    

# Write a function that computes the error measure
# TODO: Implement this
   
def lsh_error(linsrh,lsh_srh):
    sum3=0
    for i in range(len(lsh_srh)):
        sum1=0
        sum2=0
        for j in range(len(lsh_srh[i])):
            sum1=sum1+linsrh[i][j][1]
            sum2=sum2+lsh_srh[i][j][0]
        sum3=sum3+(sum1/sum2)
    return sum3/10
    #raise NotImplementedError
    

#### TESTS #####




if __name__ == '__main__':
#    unittest.main() ### TODO: Uncomment this to run tests
    # create an LSH object using lsh = LSH(k=16, L=10)
    """
    Your code here
    """
    
    linearsearchtime=[]
    lshsearchtime=[]
    lsh=LSH("patches.csv",24,10)
    A=pd.read_csv("patches.csv",header=None)
    
    lsh_srh=[]
    for i in range(100,1001,100):
        start2=timeit.default_timer()
        lsh_srh.append(lsh.lsh_search(i,3))
        print(lsh_srh)
        stop2=timeit.default_timer()
        lshsearchtime.append(stop2-start2)
    avgt2=sum(lshsearchtime)/len(lshsearchtime)
    print(avgt2)
    print("lsh")
    print(lsh_srh)
    
    linsrh=[]
    for i in range(100,1001,100):
        start=timeit.default_timer()
        linsrh.append(linear_search(A,i,3))
        stop=timeit.default_timer()
        linearsearchtime.append(stop-start)
    print("lin")
    print(linsrh)
    avgtime_linear=sum(linearsearchtime)/len(linearsearchtime)
    print(avgtime_linear)
    
    errorvals=lsh_error(linsrh,lsh_srh)
    print(errorvals)
    
    B = np.genfromtxt ('patches.csv', delimiter=",")
    p1=[]
    p2=[]
    for i in range(len(linsrh[0])):
        p1.append(linsrh[0][i][0])
    
    for j in range(len(lsh_srh[0])):
        p2.append(lsh_srh[0][j][1])
    
    #plotting 10 nearesr neighbors for query index 100
    plot(B, p1, "linear")
    plot(B, p2, "lsh")
    
    
    k = 24
    L = [10,12,14,16,18,20]
    l_error = []
    for i in range(len(L)):
        lsh=LSH("patches.csv",k=k,L=L[i])
        A=pd.read_csv("patches.csv",header=None)
    
        lsh_srh=[]
        for i in range(100,1001,100):
            start2=timeit.default_timer()
            lsh_srh.append(lsh.lsh_search(i,3))
            print(lsh_srh)
            stop2=timeit.default_timer()
            lshsearchtime.append(stop2-start2)
        avgt2=sum(lshsearchtime)/len(lshsearchtime)
        print(avgt2)
        print("lsh")
        print(lsh_srh)
        
        linsrh=[]
        for i in range(100,1001,100):
            start=timeit.default_timer()
            linsrh.append(linear_search(A,i,3))
            stop=timeit.default_timer()
            linearsearchtime.append(stop-start)
        print("lin")
        print(linsrh)
        avgtime_linear=sum(linearsearchtime)/len(linearsearchtime)
        print(avgtime_linear)

        errorvals=lsh_error(linsrh,lsh_srh)
        l_error.append(errorvals)
        
    L = 10
    k = [16,18,20,22,24]
    k_error = []
    for i in range(len(k)):
    
        lsh=LSH("patches.csv",k=k[i],L=L)
        A=pd.read_csv("patches.csv",header=None)

        lsh_srh=[]
        for i in range(100,1001,100):
            start2=timeit.default_timer()
            lsh_srh.append(lsh.lsh_search(i,3))
            print(lsh_srh)
            stop2=timeit.default_timer()
            lshsearchtime.append(stop2-start2)
        avgt2=sum(lshsearchtime)/len(lshsearchtime)
        print(avgt2)
        print("lsh")
        print(lsh_srh)
        
        linsrh=[]
        for i in range(100,1001,100):
            start=timeit.default_timer()
            linsrh.append(linear_search(A,i,3))
            stop=timeit.default_timer()
            linearsearchtime.append(stop-start)
        print("lin")
        print(linsrh)
        avgtime_linear=sum(linearsearchtime)/len(linearsearchtime)
        print(avgtime_linear)

        errorvals=lsh_error(linsrh,lsh_srh)
        k_error.append(errorvals)
        
    #plotting k vs error
    plt.plot([16,18,20,22,24],k_error)
    plt.xlabel("function of K")
    plt.ylabel("Error")
    plt.title("K vs Error")
    plt.show()
    
    
    #plotting L vs error
    plt.plot([10,12,14,16,18,20],l_error)
    plt.xlabel("function of L")
    plt.ylabel("Error")
    plt.title("L vs Error")
    plt.show()
    
    





