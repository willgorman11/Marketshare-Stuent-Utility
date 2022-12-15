using CSV 
using DataFrames
using Statistics
using StatsPlots
using GR

data = CSV.read("CSanalysis.csv", DataFrame)

# Only use column id, year, cs, s, outsideopt. sort data by year and id.
da = select(data, [:id, :year, :cs, :s, :outsideopt])
da = sort(da, [:year,:id])


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
            num += 1
           
        
        end
    end
    return (time, id, result)
end

#Put results of marketshare function in dataframe and sorts by year then id
Market1, Market2, Market3 = S(da, p_cs, p_col)
Market = DataFrame(year=Market1, id=Market2, sj=Market3)
Market = sort(Market, [:year, :id])

#################################################

####### As p_cs and p_col vary ##################
year = deepcopy(da.year)
damarket = DataFrame(year=unique(year))

l = range(0.1, 0.9, 20)
    for i = 1:length(l)
        p_col = l[i]
        p_cs = 1-p_col
        
        #println(p_col, p_cs)
        gt1, gt2 = G(da, p_cs, p_col)
        ga = DataFrame(year=gt1, G=gt2)

        Market1, Market2, Market3 = S(da, p_cs, p_col)
        Market = DataFrame(year=Market1, id=Market2, sj=Market3)
        Market = sort(Market, [:year, :id])

        market_avg = Market
        market_avg = combine(groupby(market_avg, [:year]), df -> mean(df.sj))
        rename!(market_avg, :x1 => :avg)

        #f = @df market_avg StatsPlots.plot(:year, :avg, label = "Line$i")
        #function isn't very clean - plot! will produce confusing graphs if used multiple times in a row
        #the code below shows this graph for each iteration - the last graph shows all these plots on the same graph
        #If we use plot instead of plot! we can just see each iteration on its own seperate graph
        #display(f)
    end


######### As delta varies ######################

d = range(-1, 1, 12)
    for i = 1:length(d)

        da = select(data, [:id, :year, :cs, :s, :outsideopt])
        da = sort(da, [:year,:id])
    
        delta = select(da, [:s, :outsideopt] => (x, y) -> log.(x).-log.(y))
        delta = delta .+ d[i]
        #println(d[i])
        insertcols!(da, 6, :delta => delta.s_outsideopt_function)
    
        gt1, gt2 = G(da, p_cs, p_col)
        ga = DataFrame(year=gt1, G=gt2)

        Market1, Market2, Market3 = S(da, p_cs, p_col)
        Market = DataFrame(year=Market1, id=Market2, sj=Market3)
        Market = sort(Market, [:year, :id])

        market_avg = Market
        market_avg = combine(groupby(market_avg, [:year]), df -> mean(df.sj))
        rename!(market_avg, :x1 => :avg)

        #h = @df market_avg StatsPlots.plot(:year, :avg, label = "Line$i")
        #function isn't very clean - plot! will produce confusing graphs if used multiple times 		in a row
        #the code below shows this graph for each iteration - the last graph shows all these 			plots on the same graph
        #If we use plot instead of plot! we can just see each iteration on its own seperate 			graph
        #display(h)
        
    end



