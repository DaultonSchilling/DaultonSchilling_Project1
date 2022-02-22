import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

object Project1Ex {
    def main(args: Array[String]): Unit = {
       
        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\") 
        val conf = new SparkConf()
            .setMaster("local") 
            .setAppName("Project1")    
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val hiveCtx = new HiveContext(sc)
        import hiveCtx.implicits._

         val driver = "com.mysql.cj.jdbc.Driver"
         val url = "jdbc:mysql://localhost:3306/MyDB" 
         val username_Main = "root"
         val password_Main = "jared24" 
       
        var connection:Connection = null

        Class.forName(driver)
        connection = DriverManager.getConnection(url, username_Main, password_Main)

        var OnMainMenu = true

       insertFilmData(hiveCtx)//******************
       //MostCommonYear(hiveCtx)
       //FiveMostCommonGenres(hiveCtx)
       //StudioWithTheMostFilms(hiveCtx)
       //Top5HighestAudienceScores(hiveCtx)
       //Top5HighestRTScores(hiveCtx)
       //Top5HighestGrossingFilms(hiveCtx)
      
       

        def DisplayData: Unit = 
        {
            println("What year has the most amount of films?")
                MostCommonYear(hiveCtx)
                

            println("What are the five most common genres?")
                FiveMostCommonGenres(hiveCtx)
               

            println("Which studio made the most films?")
                StudioWithTheMostFilms(hiveCtx)
                

            println("What are the five films with the higest audience score?")
                Top5HighestAudienceScores(hiveCtx)
               

            println("What are the five films with the higest Rotten-Tomatoe score?")
                Top5HighestRTScores(hiveCtx)
                

            println("What are the top 5 highest grossing films?")
                Top5HighestGrossingFilms(hiveCtx)

            LoggedIn
                

            

        }

        def LoggedIn: Unit = 
        {
            var input = ""

            println("1) Edit Account details")
            println("2) Delete Account")

            println("3) Display Film Data***")
            println("4) Log out")

            input = scala.io.StdIn.readLine()

            if(input == "1")
            {
                    var UpdatingAccountDetails = true
                    
                    println("New UserName: ")

                    while(UpdatingAccountDetails == true)
                    {
                        var newPassword = ""
                        var newPasswordMatch = ""

                        var oldPassword = ""
                        var newUsername = ""
                        var scanner = new Scanner(System.in)
                   
                        
                        scanner = new Scanner(System.in)
                        newUsername = scanner.nextLine().trim()
                   
                    
                        if(newUsername != "")
                        {
                            println("Current Password: ")
                            scanner = new Scanner(System.in)
                            oldPassword = scanner.nextLine().trim()

                            println("New Password: ")
                            newPassword = scanner.nextLine().trim()

                            println("Re-enter New Password: ")
                            newPasswordMatch = scanner.nextLine().trim()

                            if(newPassword == newPasswordMatch)
                            {
                                UpdateUser(newUsername, oldPassword, newPassword)
                                println("Account details updated")
                                UpdatingAccountDetails = false
                                LoggedIn
                            }   
                            else
                            {
                                println("Passwords do not match")
                                LoggedIn

                            }

                    }
                        
            }
                    
                    

                    
            }
            else
            if(input == "2")
            {
                var OnDeleteAccountMenu = true
                 println("Delete Account")
                 println("Enter Username: ")


                while(OnDeleteAccountMenu == true)
                {
                   
                        var Password = ""
                        var PasswordMatch = ""
                       
                        var Username = ""
                        var scanner = new Scanner(System.in)
                        
                        scanner = new Scanner(System.in)
                       
                        Username = scanner.nextLine().trim()
                    
                        if(Username != "")
                        {
                            println("Enter Password: ")
                            scanner = new Scanner(System.in)
                            Password = scanner.nextLine().trim()

                            println("Re-enter New Password: ")
                            PasswordMatch = scanner.nextLine().trim()

                            if(Password == PasswordMatch)
                            {
                                println("Account Deleted!")
                                DeleteAccount(Username, Password)
                                OnDeleteAccountMenu = false
                                Start
                            }   
                            else
                            {
                                println("Passwords do not match")
                                LoggedIn
                                

                            }
                        }

                }
            }
            else
            if(input == "3")
            {
                DisplayData

            }
            else
            if(input == "4")
            {
                    OnMainMenu == true
               
                    println("Logging out...")
                    println("Welcome!")
                    Start
                
                

            }

        }

        println("Welcome!")
        
        Start
        def Start: Unit = 
        {
            while(OnMainMenu == true)
            {
                
                println("1) Login")
                println("2) Create an account")
                println("3) Exit")
            

                var input_Main = scala.io.StdIn.readLine()

                if(input_Main == "1")
                {
                    println("Login")
                   
                    val adminCheck = login(connection)
                    if (adminCheck) {
                    println("Welcome Admin!")
                    OnMainMenu = false
                    LoggedIn
                    

                    } else {
                    println("Welcome User!")
                    OnMainMenu = false
                    LoggedIn
                   
                    }

                }
                else if(input_Main == "2")
                {
                    var password = ""
                    var password_Repeat = ""
                    var username = ""
                    var scanner = new Scanner(System.in)
                    OnMainMenu = false
                    var OnCreateAccountMenu = true

                    println("Create a new account")
                    println("UserName: ")

                    

                    while(OnCreateAccountMenu == true)
                    {
                        
                        username = scanner.nextLine().trim()

                        if(username != "")
                        {
                            println("Password: ")
                            password = scanner.nextLine().trim()

                            println("Re-enter password: ")
                            password_Repeat = scanner.nextLine().trim()

                            if(password_Repeat != "")
                            {
                                AddUser(username, password, password_Repeat)
                                OnCreateAccountMenu = false
                                OnMainMenu = true
                                Start
                                
                            }

                        }

                    }

                   
                    
                }
                else if(input_Main == "3")
                {
                    println("GoodBye!")
                    System.exit(0)

                }
                else
                {
                    println("Incorect Input")
                    Start

                }




            } 


        }

    def DeleteAccount(username: String, password: String) 
    {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username_Main, password_Main)
        val statement = connection.createStatement()
        
        val resultSet = statement.executeUpdate("DELETE FROM user_accounts WHERE username =" + "'" + username + "'" + "AND password = " + "'" + password +"'" + ";")
       
        
        
    }

    def UpdateUser(newUsername: String ,oldPassword: String, newPassword: String) 
    {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username_Main, password_Main)
        val statement = connection.createStatement()

        
        val resultSet = statement.executeUpdate("UPDATE user_accounts SET username = " + "'" + newUsername + "'" + "," + "password = " + "'" + newPassword + "'" + "WHERE password = " + "'" + oldPassword + "'" + ";")
        
        
        
    }
    def AddUser(username: String, password: String, c: String) 
    {

        Class.forName(driver)
        connection = DriverManager.getConnection(url, username_Main, password_Main)
        val statement = connection.createStatement()

        
         var pwMatch = true
        
            if(password != c)
            {
                println("Passwords do not match")
                pwMatch = false
            }
            else
            {
               val resultSet = statement.executeUpdate("INSERT INTO user_accounts(username, password)" + 
               "VALUES(" + "'" + username + "'" + ","  + "'" + password + "'" + ");") 

               println("Account created for " + username)
              
            }
            
      
        
        
    }
       
        sc.stop() 
    }

    
    def login(connection: Connection): Boolean = 
    {

        println("Enter username: ")
        while (true) {
            val statement = connection.createStatement()
            val statement2 = connection.createStatement()
           
            var scanner = new Scanner(System.in)
            var username = scanner.nextLine().trim()

            if(username != "")
            {
            println("Enter password: ")
            var password = scanner.nextLine().trim()
            val resultSet = statement.executeQuery("SELECT COUNT(*) FROM admin_accounts WHERE username='"+username+"' AND password='"+password+"';")
            while ( resultSet.next() ) {
                if (resultSet.getString(1) == "1") {
                    return true;
                }
            }

            val resultSet2 = statement2.executeQuery("SELECT COUNT(*) FROM user_accounts WHERE username='"+username+"' AND password='"+password+"';")
            while ( resultSet2.next() ) {
                if (resultSet2.getString(1) == "1") {
                    return false;
                }
            }

            println("Username/password combo not found. Try again!")
            println("Enter username: ")

            }
            
        }
        return false
    }


    

    def MostCommonYear(hiveCtx:HiveContext): Unit = 
    {
        val result = hiveCtx.sql("SELECT Year, COUNT(Year) AS NumberOfFilms FROM Table GROUP BY Year ORDER BY NumberOfFilms DESC LIMIT 1")
       
        result.show()
       result.write.csv("results/MostCommonYear")
    }

    def FiveMostCommonGenres(hiveCtx:HiveContext): Unit = 
    {
        val result = hiveCtx.sql("SELECT Genre, COUNT(Genre) AS NumberOfFilms FROM Table GROUP BY Genre ORDER BY NumberOfFilms DESC LIMIT 5")
        
        result.show()
        result.write.csv("results/FiveMostCommonGenres")
    }

    def StudioWithTheMostFilms(hiveCtx:HiveContext): Unit = 
    {
        val result = hiveCtx.sql("SELECT Studio, COUNT(Studio) AS NumberOfFilms FROM Table GROUP BY Studio ORDER BY NumberOfFilms DESC LIMIT 1")
        
        result.show()
        result.write.csv("results/StudioWithTheMostFilms")
    }

    def Top5HighestAudienceScores(hiveCtx:HiveContext): Unit = 
    {
        val result = hiveCtx.sql("SELECT Film, MAX(Score) AS AudienceScore FROM Table GROUP BY Film ORDER BY AudienceScore DESC LIMIT 5")
        
        
        result.show()
        result.write.csv("results/Top5HighestAudienceScores")
    }

    def Top5HighestRTScores(hiveCtx:HiveContext): Unit = 
    {
        val result = hiveCtx.sql("SELECT Film, MAX(RT) AS RTScore FROM Table GROUP BY Film ORDER BY RTScore DESC LIMIT 5")
        
        
        result.show()
        result.write.csv("results/Top5HighestRTScores")
    }

    def Top5HighestGrossingFilms(hiveCtx:HiveContext): Unit = 
    {
        val result = hiveCtx.sql("SELECT Film, MAX(Gross) AS GrossedInMillions FROM Table GROUP BY Film ORDER BY GrossedInMillions DESC LIMIT 5")
        
        
        result.show()
        result.write.csv("results/Top5HighestGrossingFilms")
    }
    
    
    def insertFilmData(hiveCtx:HiveContext): Unit = 
    {
        hiveCtx.sql("DROP TABLE IF EXISTS Table")
        hiveCtx.sql("DROP TABLE IF EXISTS data1")

         val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("input/DataSource-Film.csv")
        //output.limit(10).show() 

     
        output.createOrReplaceTempView("temp_data")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS Table (Film STRING, Genre STRING, Studio STRING, Score INT, Profit STRING, RT INT, Gross DECIMAL, Year INT)")
        hiveCtx.sql("INSERT INTO Table SELECT * FROM temp_data")
        
        val summary = hiveCtx.sql("SELECT * FROM Table LIMIT 10")


        //summary.show()
    }

    

}//END