const phoneScreen = window.matchMedia('(max-width: 576px)');
const btnStart = document.querySelector(".btn-start");
const warning =  document.querySelector(".warningLocationUser");
const btnConfirmLocation =  document.querySelector(".confirmLocation");
const btnDeniedLocation = document.querySelector(".deniedLocation");
const citiesOption = document.querySelector('.option.cities');
const navBar = document.querySelector(".header");
const Loader = document.querySelector(".containerLoad")
const InitScreen = document.querySelector(".init")
const weatherComponent = document.querySelector(".weatherComponent")
const cityComponent = document.querySelector(".cityComponent")
const mapComponent = document.querySelector(".mapComponent")
const profilComponent = document.querySelector(".profilComponent")
const childnodeListTabs = Array.from(navBar.childNodes).filter(card => card.tagName === "DIV");
const btnSearchCity = document.querySelector(".btnSearch")
const inputSeachCity = document.querySelector('.inputCitySearch');

var forecastHours = {
    sixAm : "",
    nineAm: "",
    twelveAm:"",
    threePm:"",
    sixPm:"",
    ninePm:""
};
var descricaoClima = {
    
    "clear sky":            `./static/img/ensolarado.png`,
    "few clouds":           `./static/img/ensolarado.png`,
    "scattered clouds":     `./static/img/nuvens-esparças.png`, 
    "broken clouds":        `./static/img/parcialmente-nublado.png`,
    "overcast clouds":      `./static/img/parcialmente-nublado-.png`,
    "light rain":           `./static/img/chuva-leve-1.png`,
    "moderate rain":        `./static/img/chuva-leve-2.png`,
    "heavy intensity rain": `./static/img/chuva-pesada-3.png`,
    "very heavy rain":      `./static/img/chuva-intensa-4.png`,
    "extreme rain":         `./static/img/chuva-torrencial-5.png`,
    "thunderstorm":         `./static/img/storm.png`,
    "snow":                 `./static/img/neve.png`,
    "mix snow/rain":        `./static/img/chuva-com-neve.png`,
    "mist":                 `./static/img/nevoa.png`,
    "haze":                 `./static/img/fumaça.png`,
    "smoke":                `./static/img/nevoa.png`,
    "dust":                 `./static/img/poeira.png`,
    "sand":                 `./static/img/sand.png`,
    "volcanic ash":         `./static/img/cinzas-vulcão.png`,
    "squalls":              `./static/img/clima-ventoso.png`,
    "tornado":              `./static/img/tornado.png`

};
var descricaoClimaPt = {
    "clear sky": "clear sky",            
    "few clouds": "few clouds",     
    "scattered clouds": "scattered clouds",  
    "broken clouds": "broken clouds",  
    "overcast clouds": "overcast clouds",  
    "light rain": "light rain",  
    "moderate rain": "moderate rain",  
    "heavy intensity rain": "heavy intensity rain",  
    "very heavy rain": "very heavy rain",  
    "extreme rain": "extreme rain",  
    "thunderstorm": "thunderstorm",  
    "snow": "snow",  
    "mix snow/rain": "mix snow/rain",  
    "mist": "mist",  
    "mist": "mist",  
    "smoke": "smoke",  
    "dust": "dust",  
    "sand": "sand",  
    "volcanic ash": "volcanic ash", 
    "squalls": "squalls",  
    "tornado": "tornado",  
};
var nameCityData, city,
    AirUmidityData,
    temperatureData, 
    thermalSensationData, 
    windSpeedData, 
    climate,
    visibilityData,
    latitude, 
    longitude,
    weather = navBar.children[1].children[1],
    cities = navBar.children[2].children[1],
    map = navBar.children[3].children[1],
    profil =navBar.children[4].children[1];

    citiesOption.addEventListener('click', () => {
        loadHistory();
    });
    
btnSearchCity.addEventListener('click',createNewcity)
btnConfirmLocation.addEventListener('click', checkPermission)
btnStart.addEventListener('click', showWarning)
btnDeniedLocation.addEventListener('click', ()=>{
    toggleTab('InitScreen','weather',true);
})
inputSeachCity.addEventListener('keypress',(event)=>{
    if (event.keyCode === 13){createNewcity()}

})
phoneScreen.addEventListener("change", () => {
    removeTextBtnStart();
    removeTextBtnNav();
    increaseScreenSize();
});
childnodeListTabs.forEach((tab)=>{
    //Responsável pela troca de tabs
    tab.addEventListener('click', ()=>{
        let currentTab = checkCurrentTab();
        let showheader = true;
        let newTab;

        childnodeListTabs.forEach((i)=>{i.classList.remove("active")})
        tab.classList.add("active")
        newTab = tab.classList[1] 

        toggleTab(currentTab,newTab,showheader)
        increaseScreenSize()
    })
    
})

//Resolver bug adicionandon uma condicionaçde tamanho
function removeTextBtnNav(){
    if(phoneScreen.matches){
        weather.innerText ="";
        cities.innerText ="";
        map.innerText ="";
        profil.innerText ="";
    }else{
        weather.innerText ="Weather";
        cities.innerText ="Cities";
        map.innerText ="Map";
        profil.innerText ="Profil";
    }

}
function checkCity( city){
    console.log(city)
    // Get cities saved to local history into cities array
    getSearchHistory();

    // Add the city to the local storage array
    if (cities.includes(city)) {
        console.log('yes')
        return true;
    } else {
        return false;
    }
}
function removeTextBtnStart() {
    if (phoneScreen.matches) {
      btnStart.innerText = "";
    } else {
      btnStart.innerText = "Start";
    }
}
function increaseScreenSize()  {   
    let main = document.querySelector("main") 
    let body = document.querySelector("body") 

    if(childnodeListTabs[0].classList.contains("active")){
        main.classList.add("citiesHeight")
        body.classList.add("citiesHeight")
    }else{
        main.classList.remove("citiesHeight")
        body.classList.remove("citiesHeight")
    }

}
function start() {

    Loader.style.display= "none"
    InitScreen.style.display= "flex"
}
function checkPermission() {
   if (navigator.geolocation) {
    navigator.geolocation.getCurrentPosition(function(position) {
    // confirm
        latitude = position.coords.latitude;
        longitude = position.coords.longitude;
        warning.style.display="none";
        toggleTab('InitScreen','weather',true);
        makeRequisitionCity()
        makeRequisitionForecast()
        
    }, function(error) {
        // denied

            if (error.code === 1) {

                let textBoxWarning = warning.childNodes[1].children[0];
                
                textBoxWarning.innerText = "Grant access to your location for a better experience"
                setTimeout(()=>{ btnDeniedLocation.style.display="flex"; },5000)
                setTimeout(()=>{ btnDeniedLocation.style.opacity= 0.7; },5200)

            } else if (error.code === 2) {

                alert("Could not obtain your location, view in static mode");

            }
        });

    } else {
        alert("Your browser does not support geolocation");
    }
}
function showWarning() {

    warning.style.display= "flex";

}
function toggleTab(currentTab,newTab,showheader){
    
    switch (currentTab) {
        case 'InitScreen':
            currentTab = InitScreen;
            childnodeListTabs[0].classList.add("active")
            break;
        case 'weather':
            currentTab = weatherComponent;
            break;
        case 'cities':
            currentTab = cityComponent;
            break;
        case 'map':
            currentTab = mapComponent;
            break;
        case 'profil':
            currentTab = profilComponent;
            break;
        default:
            break;
    }

    switch (newTab) {
        case 'InitScreen':
            newTab = InitScreen;
            break;
        case 'weather':
            newTab = weatherComponent;
            increaseScreenSize()
            break;
        case 'cities':
            newTab = cityComponent;
            break;
        case 'map':
            newTab = mapComponent;
            break;
        case 'profil':
            newTab = profilComponent;
            break;
        default:
            break;
    }
    
    currentTab.style.display="none"
    Loader.style.display="flex"

    setTimeout(()=>{
        Loader.style.display="none";
        newTab.style.display="grid";
        
        if(showheader == true){
            warning.style.display="none" 
            navBar.style.display="flex"
        }
    },500)
   
}
function getLocation() {

    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(function(position) {
          latitude = position.coords.latitude;
          longitude = position.coords.longitude;
          console.log("Latitude: " + latitude + ", Longitude: " + longitude);
        });
    } else {
      
    }



}
function checkCurrentTab(){
    let elemento,arrayOfClass;

    childnodeListTabs.forEach((el)=>{
        if(el.classList.contains("active")){
            arrayOfClass =  Array.from(el.classList)
        }
    })
    
    switch (arrayOfClass[1]) {
        case 'weather':
        elemento = 'weather'
        break;
        case 'cities':
        elemento = 'cities'
        break;
        case 'map':
        elemento = 'map'
        break;
        case 'profil':
        elemento = 'profil'
        break;
        default:
        break;
    }
      

    return elemento 
}
async function makeRequisitionCity(){
    
    const dados = await fetch(`https://nominatim.openstreetmap.org/reverse?lat=${latitude}&lon=${longitude}&format=json`)
    let response = await dados.json()
    let city = response.address.city;
    
    makeRequisitionWeather(city)

}
async function makeRequisitionWeather(city) {
    if(city===undefined)
        city='London'
    // let Apikey = "5182a2574871dbd140787ce3dc109c97";
    // const data = await fetch(`https://api.openweathermap.org/data/2.5/weather?q=${city}&units=metric&lang=pt_br&mode=json&appid=${Apikey}`)
    const data = await fetch(`http://127.0.0.1:5002/currentWeatherData/${city}`)
    console.log(data)
    let response = await data.json();
    console.log(response)
    dataProcessingWeather(response);
    

}
function dataProcessingWeather(data){


    nameCityData = data.name;
    temperatureData = parseInt(parseFloat(data.temp)- 273.15);
    climate = data.description;

    thermalSensationData = parseFloat(data.feels_like);
    windSpeedData = data.speed;
    AirUmidityData = data.humidity;
    visibilityData = data.visibility;

    async function changeValuesDisplay(){
        
        await data

        let visibility = document.querySelector(".Visibilidade");
        let AirUmidity = document.querySelector(".Umidade");
        let windSpeed = document.querySelector(".velocidade-vento");
        let thermalSensation = document.querySelector(".sensacao-termica");
    
        let nameCity = document.querySelector(".cityName");
        let climateCity = document.querySelector(".climateCity");
        let temperature = document.querySelector(".graus");
    
        visibility.innerHTML = visibilityData;
        AirUmidity.innerHTML = `${AirUmidityData}°`;
        windSpeed.innerHTML = windSpeedData;
        thermalSensation.innerHTML =`${thermalSensationData}°`;

        nameCity.innerHTML = nameCityData;
        climateCity.innerHTML = climate;
        temperature.innerHTML = `${temperatureData}°`;
    }

    changeValuesDisplay()
 
}
async function  makeRequisitionForecast(){
    console.log("aaaaaaaaaaaaaa")
    // let Apikey = "5182a2574871dbd140787ce3dc109c97";
    // const data = await fetch(`https://api.openweathermap.org/data/2.5/forecast?lat=${latitude}&lon=${longitude}&units=metric&appid=${Apikey}`)
    const data = await fetch(`http://127.0.0.1:5002/forecastWeatherData/${city}`)
    let response = await data.json();
    console.log(response)
    dataProcessingForecast(response)
    dataProcessing5DaysForecast(response)
    
}
async function dataProcessingForecast(response){

    const dados = await response;
    console.log(response)
    forecastHours.sixAm = dados.list[1].weather[0].description;
    forecastHours.nineAm = dados.list[2].weather[0].description;
    forecastHours.twelveAm = dados.list[3].weather[0].description;
    forecastHours.threePm = dados.list[4].weather[0].description;
    forecastHours.sixPm = dados.list[5].weather[0].description;
    forecastHours.ninePm = dados.list[6].weather[0].description;
    console.log(forecastHours.sixAm)
    
    for (const chave in forecastHours) {
        for (const descricao in descricaoClima) {
            if (forecastHours[chave] === descricao) {
                forecastHours[chave] = descricaoClima[descricao];
                break;
            }
        }
    }
    async function changeForecastDisplay(){
        
        await response
        console.log("forecast update")
        let imgSixAm = document.querySelector(".imgSixAm");
        let imgNineAm = document.querySelector(".imgNineAm");
        let imgTwelveAm = document.querySelector(".imgTwelveAm");
        let imgThreePm = document.querySelector(".imgThreePm");
        let imgSixPm = document.querySelector(".imgSixPm");
        let imgNinePm = document.querySelector(".imgNinePm");

        imgSixAm.src = forecastHours.sixAm;
        imgNineAm.src = forecastHours.nineAm;
        imgTwelveAm.src = forecastHours.twelveAm;
        imgThreePm.src = forecastHours.threePm;
        imgSixPm.src = forecastHours.sixPm;
        imgNinePm.src = forecastHours.ninePm;
        console.log(imgSixAm.src)
    }

    changeForecastDisplay()
   
}
async function dataProcessing5DaysForecast(response) {
    
    await response;

  
    function separateDaysFromTheList(){

        const dailyAverages = {};
        let dataWork = response.list;
        
        
        dataWork.forEach((item) => {
          const date = new Date(item.dt_txt);
          const day = date.getDate();
          
          if (!dailyAverages[day]) {
            dailyAverages[day] = [];
          }
          
          dailyAverages[day].push(item);
        });

        let maioresQueHoje = []
        let menoresQueHoje = []
        let dadosDoDiaAtual = []
        let dailyAveragesOrg = []
        let dataDeHoje = new Date().getDate();
        
        for(let index = 0;index < 6; index++){

            if(Object.entries(dailyAverages)[index][0] > dataDeHoje ){
                maioresQueHoje.push(Object.entries(dailyAverages)[index])
            }
            if(Object.entries(dailyAverages)[index][0] < dataDeHoje ){
                menoresQueHoje.push(Object.entries(dailyAverages)[index])
            }
            if(Object.entries(dailyAverages)[index][0] == dataDeHoje ){
                dadosDoDiaAtual.push(Object.entries(dailyAverages)[index])
            }
        } 

        dadosDoDiaAtual.forEach(item =>{ dailyAveragesOrg.push(item)})
        maioresQueHoje.forEach(item =>{ dailyAveragesOrg.push(item)})
        menoresQueHoje.forEach(item =>{ dailyAveragesOrg.push(item)})

        return dailyAveragesOrg

    }

    function separateHoursFormTheList(){
        let hoursInDay = {};
        let day1 = [];
        let day2 = [];
        let day3 = [];
        let day4 = [];
        let day5 = [];
        let day6 = [];
        
        let dataWork = separateDaysFromTheList();

        dataWork[0][1].forEach((hoursList)=>{
            const data = new Date(hoursList.dt_txt);

            const hours  = {
                tempo: hoursList.weather[0].description,
                horario: data.getHours().toString().padStart(2, "0")
            };

            day1.push(hours)
        })
        dataWork[1][1].forEach((hoursList)=>{
            const data = new Date(hoursList.dt_txt);

            const hours  = {
                tempo: hoursList.weather[0].description,
                horario: data.getHours().toString().padStart(2, "0")
            };

            day2.push(hours)
        })
        dataWork[2][1].forEach((hoursList)=>{
            const data = new Date(hoursList.dt_txt);

            const hours  = {
                tempo: hoursList.weather[0].description,
                horario: data.getHours().toString().padStart(2, "0")
            };

            day3.push(hours)
        })
        dataWork[3][1].forEach((hoursList)=>{
            const data = new Date(hoursList.dt_txt);

            const hours  = {
                tempo: hoursList.weather[0].description,
                horario: data.getHours().toString().padStart(2, "0")
            };

            day4.push(hours)
        })
        dataWork[4][1].forEach((hoursList)=>{
            const data = new Date(hoursList.dt_txt);

            const hours  = {
                tempo: hoursList.weather[0].description,
                horario: data.getHours().toString().padStart(2, "0")
            };

            day5.push(hours)
        })
        dataWork[5][1].forEach((hoursList)=>{
            const data = new Date(hoursList.dt_txt);

            const hours  = {
                tempo: hoursList.weather[0].description,
                horario: data.getHours().toString().padStart(2, "0")
            };

            day6.push(hours)
        })
        
        
        hoursInDay["Dia_1"] = day1;
        hoursInDay["Dia_2"] = day2;
        hoursInDay["Dia_3"] = day3;
        hoursInDay["Dia_4"] = day4;
        hoursInDay["Dia_5"] = day5;
        hoursInDay["Dia_6"] = day6;


       return hoursInDay


    }

    return change5DaysForecastInDisplay(separateHoursFormTheList(), separateDaysFromTheList())
    
}
async function change5DaysForecastInDisplay(clima,array){

    await array;
    

    dataFirstDay = clima['Dia_1'];
    dataSecoundDay = clima['Dia_2'];
    dataThirdDay = clima['Dia_3'];
    dataFourtDay = clima['Dia_4'];
    dataFifthtDay = clima['Dia_5'];

    dateFirstDday = Object.keys(array)[0].toString()
    dateSecoundDday = Object.keys(array)[1].toString()
    dateThirdDday = Object.keys(array)[2].toString()
    dateFourthDday = Object.keys(array)[3].toString()
    dateFifthtDday = Object.keys(array)[4].toString()

    
    Object.keys(dataFirstDay).forEach((item)=>{

     //pegar as informações de cada horario e clima
        
    });



    createCards(clima)

}
async function createCards(i){

    await i 

    let day1_html = document.querySelector(".day1");
    let day2_html = document.querySelector(".day2");
    let day3_html = document.querySelector(".day3");
    let day4_html = document.querySelector(".day4");
    let day5_html = document.querySelector(".day5");

    const cardShape = `
    <div class="dia">
        Today
    </div>

    <div class="clima">
        <img src="./static/img/risco de chuva.png" alt="Tempestade">
        <span>Storm</span>
    </div>

    <div class="data">
    01/02
    </div>
    `;

    function removeCardsStaticMode(){
        let cards = document.querySelectorAll(".hours")

        cards.forEach((card)=>{
            card.remove()
        })

        
    }
      
    async function Day1(){

        numberOfCardsFirstDay = i['Dia_1'].length;
        for (let counter = 0; counter < numberOfCardsFirstDay; counter++) {
            const hoursDiv = document.createElement("div");

            hoursDiv.className = "hours";
            hoursDiv.innerHTML = cardShape;
            day1_html.appendChild(hoursDiv);

        }
    }
    async function Day2(){

        numberOfCardsSecoundDay = i['Dia_2'].length;
        for (let counter = 0; counter < numberOfCardsSecoundDay; counter++) {
            const hoursDiv = document.createElement("div");

            hoursDiv.className = "hours";
            hoursDiv.innerHTML = cardShape;
            day2_html.appendChild(hoursDiv);

        }
    }
    async function Day3(){

        numberOfCardsThirdtDay = i['Dia_3'].length;
        for (let counter = 0; counter < numberOfCardsThirdtDay; counter++) {
            const hoursDiv = document.createElement("div");

            hoursDiv.className = "hours";
            hoursDiv.innerHTML = cardShape;
            day3_html.appendChild(hoursDiv);

        }
    }
    async function Day4(){

        numberOfCardsFourthDay = i['Dia_4'].length;
        for (let counter = 0; counter < numberOfCardsFourthDay; counter++) {
            const hoursDiv = document.createElement("div");

            hoursDiv.className = "hours";
            hoursDiv.innerHTML = cardShape;
            day4_html.appendChild(hoursDiv);

        }
    }
    async function Day5(){

        numberOfCardsFifthtDay = i['Dia_5'].length;
        for (let counter = 0; counter < numberOfCardsFifthtDay; counter++) {
            const hoursDiv = document.createElement("div");

            hoursDiv.className = "hours";
            hoursDiv.innerHTML = cardShape;
            day5_html.appendChild(hoursDiv);

        }
    }

    //revisar
    function addActiveClassInTheFirstCard(){
        let days = document.querySelectorAll(".days")

        days.forEach((dia)=>{
            dia.children[0].classList.add("active")
        })
    }

    removeCardsStaticMode()
    Day1()
    Day2()
    Day3()
    Day4()
    Day5() 
    addActiveClassInTheFirstCard()

    isrtDataInCards(i)

}
async function isrtDataInCards(data){

    let nodeListDay1 = document.querySelectorAll(".day1 .hours");
    let nodeListDay2 = document.querySelectorAll(".day2 .hours");
    let nodeListDay3 = document.querySelectorAll(".day3 .hours");
    let nodeListDay4 = document.querySelectorAll(".day4 .hours");
    let nodeListDay5 = document.querySelectorAll(".day5 .hours");

    insertDay(data['Dia_1'],nodeListDay1);
    insertDay(data['Dia_2'],nodeListDay2,1);
    insertDay(data['Dia_3'],nodeListDay3,2);
    insertDay(data['Dia_4'],nodeListDay4,3);
    insertDay(data['Dia_5'],nodeListDay5,4);

    function insertDay(dia,cardList,weekday){ 

        let dataDay = dia;
        let nodeListDay = cardList;
        
        function getDay(i) {
            if(i == undefined){i = 0}
            const dataAtual = new Date();
            const diaDaSemana = dataAtual.getDay();
            const diasDaSemana = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"] ;
            const proximoDia = diasDaSemana[(diaDaSemana + i) % 7];
        
            return proximoDia;
        }
        function getTextAndImgClima(valor){
            let climaText;
            
            for(const chave in descricaoClima) {
                if(chave == valor){
                    for(const chavePt in descricaoClimaPt){
                        if(chavePt == chave){
                            climaText = descricaoClimaPt[chavePt]
                        }
                    }
                }
            }

            return climaText
        }
        function getImgClima(valor){
            let imgClima

            for (const chave in descricaoClima) {
                if (chave == valor) {
                    imgClima = descricaoClima[chave]
                }
            }

            return imgClima
        }
        for(let index = 0; index < dataDay.length; index++ ){
            
            let childnodeListDay = Array.from(nodeListDay[index].childNodes).filter(card => card.tagName === "DIV");

            //dia da previsão
            let diaDaSemanaIsrt = getDay(weekday);
            let diaDaSemanaDiv = childnodeListDay[0];

            //horario do clima 
            let horarioDiv  = childnodeListDay[2];
            let horarioIsrt = dataDay[index].horario;

            //tipo de clima
            let climaTextIsrt = getTextAndImgClima(dataDay[index].tempo);
            let climaTextDiv = childnodeListDay[1].children[1];

            //ilustração do tipo de clima
            let climaImgIsrt = getImgClima(dataDay[index].tempo);
            let climaImgDiv = childnodeListDay[1].children[0];

            
            diaDaSemanaDiv.innerText = diaDaSemanaIsrt;
            climaImgDiv.src = climaImgIsrt;
            climaTextDiv.innerText = climaTextIsrt;
            horarioDiv.innerText = `${horarioIsrt}h`;
            
            
        }
    
    }
    
    animationCards()
}
function animationCards(){

    let day1 = document.querySelector(".day1"); 
    let day2 = document.querySelector(".day2"); 
    let day3 = document.querySelector(".day3"); 
    let day4 = document.querySelector(".day4"); 
    let day5 = document.querySelector(".day5"); 

    function animation(el){  
        let cards = Array.from(el.children).filter(card => card.tagName === "DIV");
        cards.forEach(( index)=>{

            index = 0;

            setInterval(() => {

            index++;
            cards[index -1].classList.remove('active');  

            if (index === cards.length) {
                index = 0;
            }

            cards[index].classList.add('active');

            }, 9000);

        })
    }

    animation(day1)
    animation(day2)
    animation(day3)
    animation(day4)
    animation(day5)
    
}
async function makeRequisitionTabCities(){

    let Apikey = "5182a2574871dbd140787ce3dc109c97";

    const dataFortaleza = await fetch(`https://api.openweathermap.org/data/2.5/weather?q=fortaleza&units=metric&mode=json&appid=${Apikey}`)
    let responseFortaleza = await dataFortaleza.json();

    const dataBrasilia = await fetch(`https://api.openweathermap.org/data/2.5/weather?q=Brasilia&units=metric&mode=json&appid=${Apikey}`)
    let responseBrasilia = await dataBrasilia.json();

    const dataSaoPaulo = await fetch(`https://api.openweathermap.org/data/2.5/weather?q=sao Paulo&units=metric&mode=json&appid=${Apikey}`)
    let responseSaoPaulo = await dataSaoPaulo.json();

    const dataRioDeJaneiro = await fetch(`https://api.openweathermap.org/data/2.5/weather?q=rio de janeiro&units=metric&mode=json&appid=${Apikey}`)
    let responseRioDeJaneiro = await dataRioDeJaneiro.json();

    async function changeInfoFort(){
        await responseFortaleza
        let imgClimaData
        
        let temperaturaHtml = document.querySelector('.fortaleza .temperatura span')
        let climaHtml = document.querySelector('.fortaleza .ilustacaoClimaSearch img')
        let horarioHtml = document.querySelector('.fortaleza .horario') 

        let temperaturaData = parseInt(responseFortaleza.main.temp)
        let climaData = responseFortaleza.weather[0].description
        let i = new Date();
        let horaData = i.getHours();
        for (const chave in descricaoClima) {
            if (chave == climaData) {
                imgClimaData = descricaoClima[chave]
            }
        }
        
        temperaturaHtml.innerText = `${temperaturaData}°`;
        climaHtml.src = imgClimaData
        horarioHtml.innerText = `${horaData}:00`

        
        
    }
    async function changeInfoBras(){
        await responseBrasilia
        let imgClimaData

        let temperaturaHtml =document.querySelector('.rioDeJaneiro .temperatura span')
        let climaHtml = document.querySelector('.rioDeJaneiro .ilustacaoClimaSearch img')
        let horarioHtml = document.querySelector('.rioDeJaneiro .horario')

        let temperaturaData = parseInt(responseBrasilia.main.temp)
        let climaData =responseBrasilia.weather[0].description
        let i = new Date();
        let horaData = i.getHours();
        for (const chave in descricaoClima) {
            if (chave == climaData) {
                imgClimaData = descricaoClima[chave]
            }
        }

        temperaturaHtml.innerText = `${temperaturaData}°`;
        climaHtml.src  = imgClimaData
        horarioHtml.innerText = `${horaData}:00`
    }
    async function changeInfoSaoPaulo(){
        await responseSaoPaulo
        let imgClimaData            
        let temperaturaHtml = document.querySelector('.saoPaulo .temperatura span')
        let climaHtml = document.querySelector('.saoPaulo .ilustacaoClimaSearch img')
        let horarioHtml = document.querySelector('.saoPaulo .horario')

        let temperaturaData = parseInt(responseSaoPaulo.main.temp)
        let climaData =responseSaoPaulo.weather[0].description
        let i = new Date();
        let horaData = i.getHours();
        for (const chave in descricaoClima) {
            if (chave == climaData) {
                imgClimaData = descricaoClima[chave]
            }
        }

        temperaturaHtml.innerText = `${temperaturaData}°`;
        climaHtml.src  = imgClimaData
        horarioHtml.innerText = `${horaData}:00`

    }
    async function changeInfoRiodeJan(){
        await responseRioDeJaneiro
        let imgClimaData
                
        let temperaturaHtml =document.querySelector('.brasilia .temperatura span')
        let climaHtml = document.querySelector('.brasilia .ilustacaoClimaSearch img')
        let horarioHtml = document.querySelector('.brasilia .horario')

        let temperaturaData = parseInt(responseRioDeJaneiro.main.temp)
        let climaData =responseRioDeJaneiro.weather[0].description
        let i = new Date();
        let horaData = i.getHours();
        for (const chave in descricaoClima) {
            if (chave == climaData) {
                imgClimaData = descricaoClima[chave]
            }
        }

        temperaturaHtml.innerText = `${temperaturaData}°`;
        climaHtml.src  = imgClimaData
        horarioHtml.innerText = `${horaData}:00`
    }

    changeInfoFort()
    changeInfoBras()
    changeInfoSaoPaulo()
    changeInfoRiodeJan()
}

async function makeRequisitionNewCity(value){
    let Apikey = "5182a2574871dbd140787ce3dc109c97";
    console.log("ababababaabab")
    console.log(value)
    try {
        // if(value===undefined)
        //     value='Tunisia'
        const response = await fetch(`https://api.openweathermap.org/data/2.5/weather?q=${value}&units=metric&mode=json&appid=${Apikey}`);
        // console.log(`http://127.0.0.1:5002/currentWeatherData/${value}`)
        // const response = await fetch(`http://127.0.0.1:5002/currentWeatherData/${value}`);
    
        if (!response.ok) { throw new Error('Error retrieving weather dat');}
    
       var resposta = await response.json();
    
    } catch (error) { erroNameCity(error);}
    
    return resposta
}
// async function createNewcity(){
    
//     let valueInput = document.querySelector('.inputCitySearch');
//     let containerCards = document.querySelector('.cityComponent');
//     let data

//     if(!valueInput.value  == ""){
//         data = await makeRequisitionNewCity(valueInput.value)
//         valueInput.value = ""
//     }else{
//         console.log("sem dados")
//     }
    
//     async function createCardCity(){
//         await data
//         let imgClimaData

//         let nomeData = data.name
//         let temperaturaData = parseInt(parseFloat(data.temp)- 273.15)
//         let climaData = data.description
//         let i = new Date();
//         let horaData = i.getHours();
//         for (const chave in descricaoClima) {
//             if (chave == climaData) {
//                 imgClimaData = descricaoClima[chave]
//             }
//         }

//         const cardShape = `
//         <div class="loadedCities ${nomeData}">
//             <div class="ilustacaoClimaSearch">
//                 <img src="${imgClimaData}" alt="clima">
//             </div>
//             <div class="infoCitySearch">
//                 <div class="name">${nomeData}</div>
//                 <div class="horario">${horaData}:00</div>
//             </div>
//             <div class="forecastCitySearch"></div>
//             <div class="temperatura">
//                 <span>${temperaturaData}°</span>
//             </div>
//         </div>`

//         let  ultimo = containerCards.lastElementChild;
//         let bar = containerCards.children[0];
//         ultimo.remove()
//         bar.insertAdjacentHTML('afterend', cardShape)
//     }
//     inputSeachCity.blur();
//     createCardCity()
// }   
async function createNewcity(){
    
    let valueInput = document.querySelector('.inputCitySearch');
    let containerCards = document.querySelector('.cityComponent');
    let data
    
    if(!valueInput.value  == ""){
        city = valueInput.value.toLowerCase();
        if(checkCity(city)==false ) {
            data = await makeRequisitionNewCity(valueInput.value)
            valueInput.value = ""
        }
          
    }else {
        console.log("no data")
    }
    
    async function createCardCity(){
        await data
        let imgClimaData

        let nomeData = data.name
        let temperaturaData = parseInt(data.main.temp)
        let climaData = data.weather[0].description
        let i = new Date();
        let horaData = i.getHours();
        for (const chave in descricaoClima) {
            if (chave == climaData) {
                imgClimaData = descricaoClima[chave]
            }
        }
        saveToHistory(nomeData)
        const cardShape = `
        <div class="loadedCities ${nomeData}">
            <div class="ilustacaoClimaSearch">
                <img src="${imgClimaData}" alt="clima">
            </div>
            <div class="infoCitySearch">
                <div class="name">${nomeData}</div>
                <div class="horario">${horaData}:00</div>
            </div>
            <div class="forecastCitySearch"></div>
            <div class="temperatura">
                <span>${temperaturaData}°</span>
            </div>
        </div>`

        let  ultimo = containerCards.lastElementChild;
        let bar = containerCards.children[0];
        bar.insertAdjacentHTML('afterend', cardShape)
    }
    inputSeachCity.blur();
    createCardCity()
}
function saveToHistory(city) {

    // Get cities saved to local history into cities array
    city = city.toLowerCase();
    getSearchHistory();

    // Add the city to the local storage array
    if (!cities.includes(city)) {
        cities.push(city);
    }

    // Set local storage
    setSearchHistory();
}

// Get cities saved in local storage
function getSearchHistory() {
    if (localStorage.getItem("cities") === null) {
        cities = [];
    } else {
        cities = JSON.parse(localStorage.getItem("cities"));
    }
}
// Set local storage
function setSearchHistory() {
    localStorage.setItem("cities", JSON.stringify(cities));
}
async function loadHistory(){
    
    let containerCards = document.querySelector('.cityComponent');
    getSearchHistory()
    let data
    for(let city of cities) {
        if (containerCards.querySelector(`.${city}`)) {
            continue;
        }
        data = await makeRequisitionNewCity(city);
        
        async function load(){
            await data
            let imgClimaData
    
            let nomeData = data.name
            console.log(nomeData)
            let temperaturaData = parseInt(data.main.temp)
            let climaData = data.weather[0].description
            let i = new Date();
            let horaData = i.getHours();
            for (const chave in descricaoClima) {
                if (chave == climaData) {
                    imgClimaData = descricaoClima[chave]
                }
            }
            const cardShape = `
            <div class="loadedCities ${nomeData}">
                <div class="ilustacaoClimaSearch">
                    <img src="${imgClimaData}" alt="clima">
                </div>
                <div class="infoCitySearch">
                    <div class="name">${nomeData}</div>
                    <div class="horario">${horaData}:00</div>
                </div>
                <div class="forecastCitySearch"></div>
                <div class="temperatura">
                    <span>${temperaturaData}°</span>
                </div>
            </div>`
    
            let  ultimo = containerCards.lastElementChild;
            let bar = containerCards.children[0];
            bar.insertAdjacentHTML('afterend', cardShape)
        }
        load();
    }
    
    
}
function erroNameCity(){
    let placeholder = document.querySelector(".barToSearchCity span ")
    let input = document.querySelector(".barToSearchCity")
    placeholder.innerText = " Digite um nome válido"
    placeholder.style.color= "#a91d1d"
    input.classList.add("active")


    setTimeout(()=>{
        placeholder.innerText = "Search for a city by entering its name"
        placeholder.style.color= "#ffffff94"
        input.classList.remove("active")
    },1500)
}

makeRequisitionTabCities()
removeTextBtnStart();
removeTextBtnNav();
increaseScreenSize();



