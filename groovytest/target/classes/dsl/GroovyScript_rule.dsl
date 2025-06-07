import groovy.transform.BaseScript


rule {
    metadata {
        gender = context?.student?.priority?.gender
        valid = gender != null && gender ==~ /(Male|Female)/
        println "logic.valid=$valid"
    }
}
