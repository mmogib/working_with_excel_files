module WorkWithExcelFiles
import CSV, DataFrames, XLSX

courses_monitored = ["MATH101", "MATH102","MATH105", "MATH201","MATH202","MATH302","MATH333"]
teacher_folder = "teachers"
excel_file_name_template = "$teacher_folder/[user]_students.xlsx"
csv_files_path = "csv_files/"
csv_sections = "sections.csv"

# read file names
files_names = readdir(csv_files_path)

# load sections data 
sectionDF = CSV.File(csv_sections) |> 
                    DataFrames.DataFrame |> 
                    (tDF -> tDF[in(courses_monitored).(tDF.COURSE) .& (tDF.ACTIVITY .== "LEC"),:]) |> 
                    (tDF -> DataFrames.sort(tDF, [:COURSE, :SEC])) |>
                    (tDF -> DataFrames.select!(tDF, :COURSE => :course, :SEC => (DataFrames.ByRow(s -> lpad(s, 2, "0")))  => :section, :username) ) |>
                    (tDF -> DataFrames.select!(tDF, :section, [:course, :section] => (DataFrames.ByRow((c, s) -> string(c, "-", s[end - 1:end]))) => :section_code, :username))

# load students data                    
studentsDF = DataFrames.DataFrame()
files_names .|> 
    (file -> CSV.File("$csv_files_path/$file")) .|> 
    DataFrames.DataFrame .|> 
    (ndF -> DataFrames.append!(studentsDF, ndF))


mergedDF = DataFrames.innerjoin(studentsDF, sectionDF, on=:section_code)
try
    rm(teacher_folder, force=true, recursive=true)
    mkdir(teacher_folder)   
    for user in unique(sectionDF[:username])
        tmp = DataFrames.sort(mergedDF[mergedDF.username .== user,:], [:section_code,:university_id])
        tmp[:serial] = 1:DataFrames.nrow(tmp)
        DataFrames.select!(tmp, :serial, :course_code => :course, :section, :university_id => :student_id, :english_name => :student_name)
        XLSX.writetable(replace(excel_file_name_template, "[user]" => user), collect(DataFrames.eachcol(tmp)), DataFrames.names(tmp))
        # CSV.write("teachers/student_with_section_$user.csv", tmp)
    end  
    print("saved...")
catch e 
    print(e)
    
end
end # module
