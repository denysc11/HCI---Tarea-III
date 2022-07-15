global INPUT "/Users/Hp Support/Videos/03 - Cursos/05 - Herramientas computacionales/Clase 3 - Scraping Python/Tarea"

** Datos zipcode
edit
* Paste ZIP from https://www.zipcodestogo.com/Maryland/
drop zipcodemap
bys city: gen n=_n // ordena por ciudad y le asigna un número a cada county en cada ciudad de tal manera que sepamos cuantos counties hay por ciudad
keep if n==1 // ahora nos quedamos solo con un county (el primero) por ciudad
drop n //una vez que hemos realizado el filtro podemos eliminar la variable n
compress // este código es para disminuir el peso de la base de datos de manera que no se cuelgue el programa
save "$INPUT/MD_zipcodes.dta", replace // guardamos la base de datos

** Datos crimen que bajamos de socrata
import delimited "$INPUT/crime.csv",clear
keep if year==2015 // nos quedamos solo con el año 2015
drop id crime_rate // eliminamos id y crime
split name, p(,) // separamos la variable name en dos variables 
encode name2, gen(state_n) // codificamos la variable name2 y almacenamos esto en una nueva variable state_n
keep if state_n==2 // filtramos solo para las variables que tengan código 2
drop state_n // una vez filtrado eliminamos la variable
compress
save "$INPUT/MD_crime.dta", replace

** Merge
use "$INPUT/MD_crime.dta", clear
ren name1 city // renombramos la variable name1 por city 
merge m:1 city using "$INPUT/MD_zipcodes.dta" // hacemos el merge usando a variable city como referencia
keep if _m==3 
drop _m

** Bajar shapefile y buscar este ID
gen ID=.
replace ID=1	 if county=="Allegany"
replace ID=2	 if county=="Anne Arundel"
replace ID=24	 if county=="Baltimore City"
replace ID=3	 if county=="Baltimore"
replace ID=4	 if county=="Calvert"
replace ID=5	 if county=="Caroline"
replace ID=6	 if county=="Carroll"
replace ID=7	 if county=="Cecil"
replace ID=8	 if county=="Charles"
replace ID=9	 if county=="Dorchester"
replace ID=10	 if county=="Frederick"
replace ID=11	 if county=="Garrett"
replace ID=12	 if county=="Harford"
replace ID=13	 if county=="Howard"
replace ID=14	 if county=="Kent"
replace ID=15	 if county=="Montgomery"
replace ID=16	 if county=="Prince Georges"
replace ID=17	 if county=="Queen Annes"
replace ID=19	 if county=="Somerset"
replace ID=18	 if county=="Saint Marys"
replace ID=20	 if county=="Talbot"
replace ID=21	 if county=="Washington"
replace ID=22	 if county=="Wicomico"
replace ID=23	 if county=="Worcester"


gen incident=subinstr(incident_parent_type, " ", "",.)
drop incident_parent_type
bys county year month incident: egen n_crime=sum(crime_count) // ordena por county, año, mes y tipo de incidente, luego genera una variable que se llame crime_count la cual contiene la suma de los mismos tipos de crimenes, que ocurren en el mismo mes, año y pertenezcan al mismo county.  
drop name city
bys county: egen zip_m=median(zipc) // generamos una variable que contenga un solo zipcode por county, para esto podría utilizar la mediana 
bys county year month incident: gen n=_n // como la variable n_crime que generamos nos daba el número de crimenes según tupo de incidente, hay datos que se repiten ya que los estamos generando por county y no por ciudad en específico (es por esto también que eliminamos la variable ciudad ya que ya no nos interesa), entonces, generamos un variable que nos cuente las veces que se repite y luego filtramos para cuando esta variable toma el valor de 1. 
keep if n==1


drop n 
replace incident="BreakingnEntering" if incident=="Breaking&Entering"
* Para obtener zipcodes para bajar datos weather
levelsof zip_m

save "$INPUT/MD_crime_2015.dta", replace

use "$INPUT/MD_crime_2015.dta", clear
local crimes "Assault BreakingnEntering Robbery Theft"

local y 0
foreach c of local crimes {
	local y=`y'+1
	use "$INPUT/MD_crime_2015.dta", clear
	keep if incident=="`c'"
	ren n_crime `c'
	tempfile f`y'
	save `f`y''
}

use "$INPUT/MD_crime_2015.dta", clear
bys county year month: gen n=_n
keep if n==1
forv z=1/4 {
	merge 1:1 county year month using `f`z'', nogen
}

drop incident
recode Assault BreakingnEntering Robbery Theft (.=0)
save "$INPUT/MD_crime_2015_wide.dta", replace

**
cd "$INPUT/weather"
local manyfiles : dir . files "*.csv"
display `"`manyfiles'"'

tempfile manydatasets

foreach file of local manyfiles {
	import delimited "$INPUT/weather/`file'", clear 
	split date, p()
	rename date_time1 date
	drop date_time2 
	split date, p(-)
	bys date2: egen prec_mean=mean(prec)
	keep if date3=="01"
	ren location zip_m
	capture append using "`manydatasets'"
	drop maxtempc mintempc totalsnow_cm sunhour uvindex moon_illumination moonrise moonset sunrise sunset dewpointc feelslikec heatindexc windchillc windgustkmph cloudcover humidity pressure tempc visibility winddirdegree windspeedkmph precipmm
	save "`manydatasets'", replace
}

drop date3 date_time
ren date2 mm
destring mm, gen(month)
ren date1 yyyy
destring yyyy, gen(year)
merge m:1 month year zip_m using "$INPUT/MD_crime_2015_wide.dta"
keep if _m==3
drop _m

egen yymm = concat(yyy mm)
drop yyyy mm
drop zip_m


bys ID yymm: gen n_n=_n 
keep if n_n==1
drop n_n

egen id_mes = concat(ID month)

reshape wide prec Assault BreakingnEntering Robbery Theft, i(ID) j(yymm) string



export delimited using "$INPUT/MD_crime_weather.csv", replace 

