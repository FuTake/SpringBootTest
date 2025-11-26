package threadtest.config;

import groovy.lang.Binding;

import java.util.Map;

/**
 * nothing
 *
 * @ClassName ConcurrentBinding
 * @Description
 * @Author zsks
 * @Date 2021/12/5 8:50
 * @Version 1.0
 **/
public class ConcurrentBinding extends Binding{
    private static ConcurrentBinding binding = new ConcurrentBinding();
    private static ThreadLocal<Binding> locals = new ThreadLocal<>();
    private ConcurrentBinding(){

    }

    public static ConcurrentBinding getInstance(){
        locals.remove();
        locals.set(new Binding());
        return binding;
    }

    @Override
    public Object getVariable(String name) {
        Binding binding = locals.get();
        return binding.getVariable(name);
    }

    @Override
    public void setVariable(String name, Object value) {
        Binding binding = locals.get();
        binding.setVariable(name, value);
    }

    @Override
    public Map getVariables() {
        Binding binding = locals.get();
        return binding.getVariables();
    }

    @Override
    public Object getProperty(String property) {
        Binding binding = locals.get();
        return binding.getProperty(property);
    }

    @Override
    public void setProperty(String property, Object newValue) {
        Binding binding = locals.get();
        binding.setProperty(property, newValue);
    }
}
