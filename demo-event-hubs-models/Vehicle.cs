using Avro;
using Avro.Specific;

namespace models
{
    public class Vehicle : ISpecificRecord
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string Colour { get; set; }

        public virtual object Get(int fieldPos)
        {
            return fieldPos switch
            {
                0 => Id,
                1 => Name,
                2 => Description,
                3 => Colour,
                _ => throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()"),
            };
        }

        public virtual void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    Id = (int)fieldValue;
                    break;
                case 1:
                    Name = (string)fieldValue;
                    break;
                case 2:
                    Description = (string)fieldValue;
                    break;
                case 3:
                    Colour = (string)fieldValue;
                    break;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            }
        }

        public virtual Schema Schema => Schema.Parse(SchemaDefinition);

        public static string SchemaDefinition = @"{
            ""type"":""record"",
            ""name"":""Vehicle"",
            ""fields"":[
                {""name"":""Id"",""type"":""int""},
                {""name"":""Name"",""type"":""string""},
                {""name"":""Description"",""type"":[""null"",""string""],""default"":null},
                {""name"":""Colour"",""type"":[""null"",""string""],""default"":null}
            ]
        }";
    }
}