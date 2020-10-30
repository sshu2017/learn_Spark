BEGIN{
    FS=",";
    OFS="|";
    }
    
{
    if ($1 in age){
        age_counts[$1]+=1;
        age_to_num_friends[$1]+=$2;
    }else{
        age[$1]=$1;
        age_counts[$1]=1;
        age_to_num_friends[$1]=$2;
    }

}
END{
    print("Age|Average_num_of_friends")
    for (i in age){
        print(age[i], 
              age_to_num_friends[i]/age_counts[i])
        }
    }
