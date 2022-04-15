
class Object
  macro gen_to_s(io)
    {{io.id}} << {{@type.name}} << "("
    {% for var, index in @type.instance_vars %}
      {% if index != 0 %}
        {{io.id}} << ", "
      {% end %}
      {{io.id}} << {{var.stringify}} << ": "
      {{io.id}} << @{{var.id}}
    {% end %}
    {{io.id}} << ")"
  end
end
