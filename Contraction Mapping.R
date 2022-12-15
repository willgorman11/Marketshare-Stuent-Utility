using CSV 
using DataFrames
using Statistics
using StatsPlots
using GR

data = CSV.read("CSanalysis.csv", DataFrame)

# Only use column id, year, cs, s, outsideopt. sort data by year and id.
da = select(data, [:id, :year, :cs, :s, :outsideopt])
da = sort(da, [:year,:id])
#da = unique(da, [:id, :year])

#Generate delta and add it to the da dataframe
delta = select(da, [:s, :outsideopt] => (x, y) -> log.(x).-log.(y))
insertcols!(da, 6, :delta => delta.s_outsideopt_function)

p_col = 0.1;
p_cs = 1-p_col;

function G(da, p_cs, p_col)
	#creates vector of all year, vectors for time and result for each unique (17) years
    year = deepcopy(da.year)
    time = zeros(length(unique(year)));
    result = zeros(length(unique(year)));

    # Define a_cs and a_col;
    a_cs = (1-p_cs)/(2-p_cs-p_col);
    a_col = (1-p_col)/(2-p_cs-p_col);
    num = 1;
    for each in unique(year)
        # select Cs == 1 data.
        cs = filter(x -> (x.cs == 1), da)
        non_cs = filter(x -> (x.cs == 0), da)
         
        # By year, select each observation - creating a diff dataframe for each year
        daa = filter(x -> (x.year == each), da)
        #Gens P1 for CS schools, P2 for non-cs, then sums for each year
        P1 = select(daa, [:delta, :cs] => (x, y) -> exp.(x./p_cs).*y);
        P1 = sum(P1.delta_cs_function)
        P2 = select(daa, [:delta, :cs] => (x, y) -> exp.(x./p_cs).*abs.(y .- 1));
        P2 = sum(P2.delta_cs_function)
        #creates part 1
        part1 = a_cs.*(P1.^(p_cs) .+ P2.^(p_cs));
        
        #Gens P3 and adds it as a column to daa
        P3 = select(daa, [:delta] => (x) -> exp.(x./p_col));
        insertcols!(daa, 7, :P3 => P3.delta_function)
        
        #sums P3 for each year
        #groups data in each year by id - selects each observation then sums it (doesn't 			really do anything?) then put each P3 observation to the p_col
        new = groupby(daa, :id)
        temp = combine(new, :P3 => sum)
        part21 = select(temp, [:P3_sum] => x -> x.^(p_col))
        #sums all the P3 observations for each year - 17 values
        part2 = sum(part21.P3_sum_function)
        part2 = part2*a_col
        
        #Generates a result for each year between 65 & 84
        result[num] = part1 + part2 + 1;
        time[num] = each;
        num += 1;
        
    end
    return (time, result)
end

#Makes the results of function G into a dataframe called g_func and connects the results with year, id, and s - then sorts by year
gt1, gt2 = G(da, p_cs, p_col)
ga = DataFrame(year=gt1, G=gt2)
g_func = select(da, [:year, :id, :s]);
g_func = leftjoin(g_func, ga; on=:year)
g_func = sort(g_func, [:year])

# Define function Market Share.
function S(da, p_cs, p_col)
    year = deepcopy(da.year)

    # Define results dataset.
    #Makes results 5120 long - for each point we have in our data - originally the code had 	just length(da)?
    time = zeros(length(da.id))
    result = zeros(length(da.id))
    id = zeros(length(da.id))
    csp = zeros(length(da.id))

    
    # Define a_cs and a_col;
    a_cs = (1-p_cs)/(2-p_cs-p_col);
    a_col = (1-p_col)/(2-p_cs-p_col);
    num = 1;

    for each in unique(year)
        # select Cs == 1 data.
        cs = filter(x -> (x.cs == 1), da);
        non_cs = filter(x -> (x.cs == 0), da);

        # Select each year obs.
        daa = filter(x -> (x.year == each), da);

        # Calculate P11 and P10, which indicate j = 1 and j = 0. Then sums to get a value for 		each year
        P11 = select(daa, [:delta, :cs] => (x, y) -> exp.(x./p_cs).*abs.(y.+(1-1)));
        P11 = sum(P11.delta_cs_function)
        P10 = select(daa, [:delta, :cs] => (x, y) -> exp.(x./p_cs).*abs.(y.+(0-1)));
		P10 = sum(P10.delta_cs_function)
        
        #for every observation
        for obs in 1:size(daa, 1)
            
            #j is the name of each observation
            j = daa[obs, :]
            
            #If cs=1, etc
            if j.cs == 1
                P1 = P11;
            elseif j.cs == 0
                P1 = P10;
            end
            
            #Generates part1 and part2, for each specific obsrvation
            part1 = a_cs .* (P1.^(p_cs)./P1) .* exp.(j.delta/p_cs)
            #fliters into each specifc observation
            school = filter(x -> (x.id == j.id), daa);
            P2 = select(school, [:delta] => x -> exp.(x./p_col));
            P2 = sum(P2.delta_function)
            part2 = a_col .* (P2.^p_col/P2) .* exp.(j.delta./p_col);
        
            #For each j, Ge is the value of G at it's specific year
            Ge = filter(ga -> ga.year == each, ga)
            Ge = Ge.G
            
            #divides by the value of G, and fills result, id, and time
            result[num] = (part1 + part2)/Ge[1]
            id[num] = j.id;
            time[num] = j.year;
            csp[num] = j.cs;
            num += 1
           
        
        end
    end
    return (time, id, result, csp)
end

#Put results of marketshare function in dataframe and sorts by year then id
Market1, Market2, Market3, Market4 = S(da, p_cs, p_col)
Market = DataFrame(year=Market1, id=Market2, sj=Market3, cs=Market4)
Market = sort(Market, [:year, :id])


#Two dataframes with results from our marketshare and delta function

#Contraction Mapping
    
eps = 1.0 
count = 1


while (eps > 0.00116)&(count < 75)
    #randomize values
    
    #1965
    p_cs = rand(92000:94000)
    p_cs = p_cs/100000
    p_col = 1 - p_cs
    
    #1966
    #p_cs = rand(941300000:941400000)
    #p_cs = p_cs/1000000000.0
    #p_col = 1 - p_cs
    
    #1982 
    #p_cs = rand(7810000:7820000)
    #p_cs = p_cs/10000000.0
    #p_col = 1 - p_cs
    
    #create G dataframe to be used
    gt1, gt2 = G(da, p_cs, p_col)
    ga = DataFrame(year=gt1, G=gt2)
    
    #create Market dataframe
    Market1, Market2, Market3, Market4 = S(da, p_cs, p_col)
    Market = DataFrame(year=Market1, id=Market2, sj=Market3, cs=Market4)
    Market = sort(Market, [:year, :id])
    
    combo = select(da, [:id, :s, :year, :delta, :cs])
    CMarket = leftjoin(Market, combo; on=[:id, :year, :cs])
    #Select year
    CMarket = filter(x -> (x.year == 1965), CMarket)
    
    act_s = CMarket.s
    pred_s = CMarket.sj
    
    CMarket.delta = exp.(CMarket.delta) .* (pred_s./act_s)
    eps = minimum(abs.((pred_s./act_s) .- 1))
    
    #Shows highest difference between actual and predicted
    println("eps is ", eps)
    
    #Used this to see best pcs/pcol values
    #if eps < 0.125
        #println("P_cs is ", p_cs)
        #println("p_col is ", p_col)
    #end
    count += 1
    
end
CMarket.delta = log.(CMarket.delta)

println(CMarket)




