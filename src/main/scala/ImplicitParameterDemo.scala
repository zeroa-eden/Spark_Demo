//object ImplicitParameterDemo {
//  class PersonInfoFormat(varstartSymbol: String, varendSymbol: String) {}
//
//  class Person(varname: String, varage: Int) {
//    //利用柯里化函数的定义方式，将函数的参数利用implicit关键字标识的话，在使用的时候可以不给出implicit对应的参数
//    def formatPerson()(implicitformat: PersonInfoFormat) = {
////      format.startSymbol+"Name:"+this.name+",age:"+this.age+format.endSymbol
//    }
//  }
//
//  def main(args: Array[String]): Unit = {
//    //程序中定义的变量PersonInfoFormat被称隐式值
//    implicit val personFormat = new PersonInfoFormat("[", "]")
//    val person: Person = new Person("小明", 22);
//    val personInfo = person.formatPerson()
//    println(personInfo)
//  }
//
//}
